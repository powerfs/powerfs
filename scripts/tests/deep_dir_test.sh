#!/bin/bash
# Deep directory tree test suite for PowerFS FUSE
#
# Verifies correctness of directory operations at depth, including:
#   - Deep nested directory creation (10+ levels)
#   - cp -prf (recursive copy preserving permissions)
#   - rm -rf (recursive removal)
#   - ls at various depths
#   - find traversal
#   - Cross-shard directory distribution (dirs jump shards)
#   - stat . / stat .. at depth
#   - rename deep trees
#   - Mixed file/dir trees
#
# Prerequisites:
#   - Docker cluster running with fuse-test container
#   - FUSE mounted at /mnt/fuse in fuse-test container

set -u
PASS=0
FAIL=0
FAILED_TESTS=()

MOUNT="/mnt/fuse"
TEST_DIR="$MOUNT/deep_dir_test"
FUSE="docker exec fuse-test"

log()  { echo "[$(date '+%H:%M:%S')] $1"; }
ok()   { echo "  OK: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); FAILED_TESTS+=("$1"); }

# Helper: run a test block
# Usage: test_case "name" "command"  (command must return 0 on success)
test_case() {
    local name="$1"
    local cmd="$2"
    local output
    output=$(eval "$cmd" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        ok "$name"
    else
        fail "$name"
        [ -n "$output" ] && echo "       output: $output"
    fi
}

echo "============================================================"
echo "  Deep Directory Tree Test Suite"
echo "  Tests cp -prf, rm -rf, ls, find on deep nested trees"
echo "============================================================"
echo ""

# ------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------
log "Setup: cleaning test directory..."
$FUSE rm -rf "$TEST_DIR" 2>/dev/null || true
$FUSE mkdir -p "$TEST_DIR" 2>&1 || { echo "FATAL: cannot create test dir"; exit 1; }
echo ""

# ------------------------------------------------------------------
# D1: Deep directory creation (10 levels)
# ------------------------------------------------------------------
echo "--- D1: Deep directory creation (10 levels) ---"

test_case "D1.1 mkdir -p 10-level deep tree" \
  "$FUSE bash -c 'mkdir -p $TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10 && [ -d $TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10 ]'"

test_case "D1.2 create files at each level" \
  "$FUSE bash -c 'for i in 1 2 3 4 5 6 7 8 9 10; do echo \"level_\$i\" > $TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10/file_\$i.txt; done && [ \$(ls $TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10/ | wc -l) -eq 10 ]'"

test_case "D1.3 read files at deepest level" \
  "$FUSE bash -c '[ \"\$(cat $TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10/file_5.txt)\" = \"level_5\" ]'"

test_case "D1.4 stat . at deepest level" \
  "$FUSE bash -c 'cd $TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10 && stat . > /dev/null 2>&1'"

test_case "D1.5 stat .. at deepest level" \
  "$FUSE bash -c 'cd $TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10 && stat .. > /dev/null 2>&1'"

test_case "D1.6 pwd at deepest level" \
  "$FUSE bash -c 'cd $TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10 && [ \"\$(pwd)\" = \"$TEST_DIR/d1/l1/l2/l3/l4/l5/l6/l7/l8/l9/l10\" ]'"

test_case "D1.7 cleanup D1" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d1 && ! [ -d $TEST_DIR/d1 ]'"
echo ""

# ------------------------------------------------------------------
# D2: cp -prf (recursive copy preserving permissions)
# ------------------------------------------------------------------
echo "--- D2: cp -prf (recursive copy) ---"

# Create source tree with mixed content
$FUSE bash -c "mkdir -p $TEST_DIR/d2_src/sub1/sub2 && \
  echo 'file1' > $TEST_DIR/d2_src/file1.txt && \
  echo 'file2' > $TEST_DIR/d2_src/sub1/file2.txt && \
  echo 'file3' > $TEST_DIR/d2_src/sub1/sub2/file3.txt && \
  chmod 755 $TEST_DIR/d2_src/file1.txt && \
  chmod 644 $TEST_DIR/d2_src/sub1/file2.txt && \
  chmod 600 $TEST_DIR/d2_src/sub1/sub2/file3.txt && \
  mkdir $TEST_DIR/d2_src/empty_dir"

test_case "D2.1 cp -prf source to dest" \
  "$FUSE bash -c 'cp -prf $TEST_DIR/d2_src $TEST_DIR/d2_dst && [ -d $TEST_DIR/d2_dst ]'"

test_case "D2.2 dest has same file count" \
  "$FUSE bash -c '[ \"\$(find $TEST_DIR/d2_src -type f | wc -l)\" = \"\$(find $TEST_DIR/d2_dst -type f | wc -l)\" ]'"

test_case "D2.3 dest has same dir count" \
  "$FUSE bash -c '[ \"\$(find $TEST_DIR/d2_src -type d | wc -l)\" = \"\$(find $TEST_DIR/d2_dst -type d | wc -l)\" ]'"

test_case "D2.4 file content preserved" \
  "$FUSE bash -c '[ \"\$(cat $TEST_DIR/d2_dst/file1.txt)\" = \"file1\" ] && [ \"\$(cat $TEST_DIR/d2_dst/sub1/file2.txt)\" = \"file2\" ] && [ \"\$(cat $TEST_DIR/d2_dst/sub1/sub2/file3.txt)\" = \"file3\" ]'"

test_case "D2.5 file permissions preserved" \
  "$FUSE bash -c '[ \"\$(stat -c %a $TEST_DIR/d2_dst/file1.txt)\" = \"755\" ] && [ \"\$(stat -c %a $TEST_DIR/d2_dst/sub1/file2.txt)\" = \"644\" ] && [ \"\$(stat -c %a $TEST_DIR/d2_dst/sub1/sub2/file3.txt)\" = \"600\" ]'"

test_case "D2.6 empty dir copied" \
  "$FUSE bash -c '[ -d $TEST_DIR/d2_dst/empty_dir ]'"

test_case "D2.7 cp -prf single file" \
  "$FUSE bash -c 'cp -prf $TEST_DIR/d2_src/file1.txt $TEST_DIR/d2_copied.txt && [ -f $TEST_DIR/d2_copied.txt ] && [ \"\$(cat $TEST_DIR/d2_copied.txt)\" = \"file1\" ]'"

test_case "D2.8 cp -prf over existing dir (merge)" \
  "$FUSE bash -c 'mkdir -p $TEST_DIR/d2_merge && echo old > $TEST_DIR/d2_merge/file1.txt && cp -prf $TEST_DIR/d2_src/sub1 $TEST_DIR/d2_merge/ && [ -f $TEST_DIR/d2_merge/sub1/file2.txt ]'"

test_case "D2.9 cleanup D2" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d2_src $TEST_DIR/d2_dst $TEST_DIR/d2_copied.txt $TEST_DIR/d2_merge'"
echo ""

# ------------------------------------------------------------------
# D3: rm -rf (recursive removal)
# ------------------------------------------------------------------
echo "--- D3: rm -rf (recursive removal) ---"

# Create a complex tree
$FUSE bash -c "mkdir -p $TEST_DIR/d3/a/b/c/d/e && \
  for i in 1 2 3 4 5; do echo \"f\$i\" > $TEST_DIR/d3/a/file\$i.txt; done && \
  for i in 1 2 3; do echo \"fb\$i\" > $TEST_DIR/d3/a/b/file\$i.txt; done && \
  echo deep > $TEST_DIR/d3/a/b/c/d/e/deep.txt && \
  mkdir -p $TEST_DIR/d3/x/y/z && \
  echo xfile > $TEST_DIR/d3/x/xfile.txt"

test_case "D3.1 rm -rf deep subtree" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d3/a/b/c && ! [ -d $TEST_DIR/d3/a/b/c ] && [ -d $TEST_DIR/d3/a/b ]'"

test_case "D3.2 rm -rf entire branch" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d3/x && ! [ -d $TEST_DIR/d3/x ]'"

test_case "D3.3 rm -rf non-existent (no error)" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d3/nonexistent 2>/dev/null; [ \$? -eq 0 ]'"

test_case "D3.4 rm single file from middle" \
  "$FUSE bash -c 'rm $TEST_DIR/d3/a/file3.txt && ! [ -f $TEST_DIR/d3/a/file3.txt ] && [ \$(ls $TEST_DIR/d3/a/file*.txt | wc -l) -eq 4 ]'"

test_case "D3.5 rm -rf entire tree" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d3 && ! [ -d $TEST_DIR/d3 ]'"

test_case "D3.6 rm -rf on empty dir" \
  "$FUSE bash -c 'mkdir -p $TEST_DIR/d3_empty && rm -rf $TEST_DIR/d3_empty && ! [ -d $TEST_DIR/d3_empty ]'"
echo ""

# ------------------------------------------------------------------
# D4: ls at various depths
# ------------------------------------------------------------------
echo "--- D4: ls at various depths ---"

# Create tree with known structure
$FUSE bash -c "mkdir -p $TEST_DIR/d4/subA/subB && \
  echo f1 > $TEST_DIR/d4/file1.txt && \
  echo f2 > $TEST_DIR/d4/file2.txt && \
  echo f3 > $TEST_DIR/d4/subA/file3.txt && \
  echo f4 > $TEST_DIR/d4/subA/subB/file4.txt && \
  mkdir $TEST_DIR/d4/subA/empty"

test_case "D4.1 ls top level shows files and dirs" \
  "$FUSE bash -c 'ls $TEST_DIR/d4/ | sort | paste -sd \" \" | grep -q \"file1.txt file2.txt subA\"'"

test_case "D4.2 ls -la shows . and .." \
  "$FUSE bash -c 'ls -la $TEST_DIR/d4/ | grep -qE \"^d.*\\s\\.\\$\" && ls -la $TEST_DIR/d4/ | grep -qE \"^d.*\\.\\.\\$\"'"

test_case "D4.3 ls subdirectory" \
  "$FUSE bash -c 'ls $TEST_DIR/d4/subA/ | sort | paste -sd \" \" | grep -q \"empty file3.txt subB\"'"

test_case "D4.4 ls -R recursive" \
  "$FUSE bash -c 'ls -R $TEST_DIR/d4/ 2>&1 | grep -q subB'"

test_case "D4.5 ls empty directory" \
  "$FUSE bash -c '[ -z \"\$(ls $TEST_DIR/d4/subA/empty/)\" ]'"

test_case "D4.6 ls with wildcard" \
  "$FUSE bash -c '[ \$(ls $TEST_DIR/d4/file*.txt | wc -l) -eq 2 ]'"

test_case "D4.7 ls -l shows correct file sizes" \
  "$FUSE bash -c '[ \"\$(stat -c %s $TEST_DIR/d4/file1.txt)\" -gt 0 ]'"

test_case "D4.8 cleanup D4" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d4'"
echo ""

# ------------------------------------------------------------------
# D5: find traversal
# ------------------------------------------------------------------
echo "--- D5: find traversal ---"

# Create a larger tree for find
$FUSE bash -c "mkdir -p $TEST_DIR/d5/{dir1,dir2,dir3}/{sub1,sub2} && \
  for d in dir1 dir2 dir3; do \
    for s in sub1 sub2; do \
      for i in 1 2 3; do echo \"\${d}_\${s}_\${i}\" > $TEST_DIR/d5/\$d/\$s/file_\$i.txt; done; \
    done; \
  done && \
  echo root_file > $TEST_DIR/d5/root.txt"

test_case "D5.1 find all files" \
  "$FUSE bash -c '[ \$(find $TEST_DIR/d5 -type f | wc -l) -eq 19 ]'"

test_case "D5.2 find all directories" \
  "$FUSE bash -c '[ \$(find $TEST_DIR/d5 -type d | wc -l) -eq 10 ]'"

test_case "D5.3 find by name pattern" \
  "$FUSE bash -c '[ \$(find $TEST_DIR/d5 -name 'file_1.txt' | wc -l) -eq 6 ]'"

test_case "D5.4 find by path pattern" \
  "$FUSE bash -c '[ \$(find $TEST_DIR/d5 -path '*/dir2/*' -type f | wc -l) -eq 6 ]'"

test_case "D5.5 find + exec (cat)" \
  "$FUSE bash -c 'find $TEST_DIR/d5 -name root.txt -exec cat {} \; | grep -q root_file'"

test_case "D5.6 find + delete" \
  "$FUSE bash -c 'find $TEST_DIR/d5 -name 'file_3.txt' -delete && [ \$(find $TEST_DIR/d5 -name 'file_3.txt' | wc -l) -eq 0 ] && [ \$(find $TEST_DIR/d5 -type f | wc -l) -eq 13 ]'"

test_case "D5.7 find maxdepth" \
  "$FUSE bash -c '[ \$(find $TEST_DIR/d5 -maxdepth 1 -type f | wc -l) -eq 1 ]'"

test_case "D5.8 find mindepth" \
  "$FUSE bash -c '[ \$(find $TEST_DIR/d5 -mindepth 3 -type f | wc -l) -eq 12 ]'"

test_case "D5.9 cleanup D5" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d5'"
echo ""

# ------------------------------------------------------------------
# D6: Cross-shard directory distribution
# ------------------------------------------------------------------
echo "--- D6: Cross-shard directory distribution ---"

# Directories should jump shards. Create enough dirs to verify distribution.
test_case "D6.1 create 20 nested dirs (cross-shard)" \
  "$FUSE bash -c 'for i in \$(seq 1 20); do mkdir -p $TEST_DIR/d6/dir_\$i/sub; done && [ \$(ls -d $TEST_DIR/d6/dir_*/ | wc -l) -eq 20 ]'"

test_case "D6.2 files in each dir visible" \
  "$FUSE bash -c 'for i in \$(seq 1 20); do echo \"data_\$i\" > $TEST_DIR/d6/dir_\$i/sub/file.txt; done && [ \$(find $TEST_DIR/d6 -name file.txt | wc -l) -eq 20 ]'"

test_case "D6.3 read all files correct" \
  "$FUSE bash -c 'for i in \$(seq 1 20); do [ \"\$(cat $TEST_DIR/d6/dir_\$i/sub/file.txt)\" = \"data_\$i\" ] || exit 1; done'"

test_case "D6.4 ls shows all 20 dirs" \
  "$FUSE bash -c '[ \$(ls $TEST_DIR/d6/ | grep -c dir_) -eq 20 ]'"

test_case "D6.5 rm -rf half the dirs" \
  "$FUSE bash -c 'for i in \$(seq 1 10); do rm -rf $TEST_DIR/d6/dir_\$i; done && [ \$(ls -d $TEST_DIR/d6/dir_*/ 2>/dev/null | wc -l) -eq 10 ]'"

test_case "D6.6 remaining dirs still accessible" \
  "$FUSE bash -c 'for i in \$(seq 11 20); do [ \"\$(cat $TEST_DIR/d6/dir_\$i/sub/file.txt)\" = \"data_\$i\" ] || exit 1; done'"

test_case "D6.7 cleanup D6" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d6'"
echo ""

# ------------------------------------------------------------------
# D7: rename deep trees
# ------------------------------------------------------------------
echo "--- D7: rename deep trees ---"

$FUSE bash -c "mkdir -p $TEST_DIR/d7/src/a/b/c && \
  echo data1 > $TEST_DIR/d7/src/a/b/c/file.txt && \
  echo data2 > $TEST_DIR/d7/src/a/file2.txt && \
  mkdir -p $TEST_DIR/d7/src/a/b/empty"

test_case "D7.1 rename deep subtree (same parent)" \
  "$FUSE bash -c 'mv $TEST_DIR/d7/src/a $TEST_DIR/d7/src/renamed_a && [ -d $TEST_DIR/d7/src/renamed_a/b/c ] && ! [ -d $TEST_DIR/d7/src/a ]'"

test_case "D7.2 files accessible after rename" \
  "$FUSE bash -c '[ \"\$(cat $TEST_DIR/d7/src/renamed_a/b/c/file.txt)\" = \"data1\" ] && [ \"\$(cat $TEST_DIR/d7/src/renamed_a/file2.txt)\" = \"data2\" ]'"

test_case "D7.3 empty dir survived rename" \
  "$FUSE bash -c '[ -d $TEST_DIR/d7/src/renamed_a/b/empty ]'"

test_case "D7.4 rename to different parent (cross-shard)" \
  "$FUSE bash -c 'mkdir -p $TEST_DIR/d7/dst && mv $TEST_DIR/d7/src/renamed_a $TEST_DIR/d7/dst/moved_a && [ -d $TEST_DIR/d7/dst/moved_a/b/c ] && ! [ -d $TEST_DIR/d7/src/renamed_a ]'"

test_case "D7.5 files accessible after cross-shard rename" \
  "$FUSE bash -c '[ \"\$(cat $TEST_DIR/d7/dst/moved_a/b/c/file.txt)\" = \"data1\" ]'"

test_case "D7.6 ls after rename shows correct structure" \
  "$FUSE bash -c 'find $TEST_DIR/d7/dst/moved_a -type f | wc -l | grep -q 2'"

test_case "D7.7 rename file within deep tree" \
  "$FUSE bash -c 'mv $TEST_DIR/d7/dst/moved_a/b/c/file.txt $TEST_DIR/d7/dst/moved_a/b/c/renamed.txt && [ -f $TEST_DIR/d7/dst/moved_a/b/c/renamed.txt ] && ! [ -f $TEST_DIR/d7/dst/moved_a/b/c/file.txt ]'"

test_case "D7.8 cleanup D7" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d7'"
echo ""

# ------------------------------------------------------------------
# D8: Mixed file/dir operations (stress)
# ------------------------------------------------------------------
echo "--- D8: Mixed operations (stress) ---"

test_case "D8.1 create mixed tree 50 files + 10 dirs" \
  "$FUSE bash -c 'for i in \$(seq 1 10); do mkdir -p $TEST_DIR/d8/dir_\$i; for j in 1 2 3 4 5; do echo \"d\${i}f\${j}\" > $TEST_DIR/d8/dir_\$i/file_\$j.txt; done; done && [ \$(find $TEST_DIR/d8 -type f | wc -l) -eq 50 ] && [ \$(find $TEST_DIR/d8 -type d | wc -l) -eq 11 ]'"

test_case "D8.2 cp -prf mixed tree" \
  "$FUSE bash -c 'cp -prf $TEST_DIR/d8 $TEST_DIR/d8_copy && [ \$(find $TEST_DIR/d8_copy -type f | wc -l) -eq 50 ]'"

test_case "D8.3 verify copied content" \
  "$FUSE bash -c 'for i in 1 5 10; do for j in 1 3 5; do [ \"\$(cat $TEST_DIR/d8_copy/dir_\$i/file_\$j.txt)\" = \"d\${i}f\${j}\" ] || exit 1; done; done'"

test_case "D8.4 modify original, copy unchanged" \
  "$FUSE bash -c 'echo modified > $TEST_DIR/d8/dir_1/file_1.txt && [ \"\$(cat $TEST_DIR/d8_copy/dir_1/file_1.txt)\" = \"d1f1\" ]'"

test_case "D8.5 rm -rf original, copy survives" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d8 && ! [ -d $TEST_DIR/d8 ] && [ -d $TEST_DIR/d8_copy ]'"

test_case "D8.6 find on copied tree" \
  "$FUSE bash -c '[ \$(find $TEST_DIR/d8_copy -name \"file_3.txt\" | wc -l) -eq 10 ]'"

test_case "D8.7 cleanup D8" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d8_copy'"
echo ""

# ------------------------------------------------------------------
# D9: stat . and stat .. at various depths
# ------------------------------------------------------------------
echo "--- D9: stat . and stat .. at various depths ---"

$FUSE bash -c "mkdir -p $TEST_DIR/d9/a/b/c/d/e"

test_case "D9.1 stat . at root" \
  "$FUSE bash -c 'cd $TEST_DIR/d9 && stat . > /dev/null 2>&1'"

test_case "D9.2 stat . at depth 5" \
  "$FUSE bash -c 'cd $TEST_DIR/d9/a/b/c/d/e && stat . > /dev/null 2>&1'"

test_case "D9.3 stat .. at depth 5" \
  "$FUSE bash -c 'cd $TEST_DIR/d9/a/b/c/d/e && stat .. > /dev/null 2>&1'"

test_case "D9.4 cd .. chain from depth 5 to root" \
  "$FUSE bash -c 'cd $TEST_DIR/d9/a/b/c/d/e && cd .. && cd .. && cd .. && cd .. && cd .. && [ \"\$(pwd)\" = \"$TEST_DIR/d9\" ]'"

test_case "D9.5 stat . after cd chain" \
  "$FUSE bash -c 'cd $TEST_DIR/d9/a/b/c/d/e && cd .. && cd .. && cd .. && cd .. && cd .. && stat . > /dev/null 2>&1'"

test_case "D9.6 stat . on empty dir" \
  "$FUSE bash -c 'mkdir -p $TEST_DIR/d9/empty && cd $TEST_DIR/d9/empty && stat . > /dev/null 2>&1'"

test_case "D9.7 stat .. from empty dir" \
  "$FUSE bash -c 'cd $TEST_DIR/d9/empty && stat .. > /dev/null 2>&1'"

test_case "D9.8 cleanup D9" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d9'"
echo ""

# ------------------------------------------------------------------
# D10: Very deep tree (20 levels)
# ------------------------------------------------------------------
echo "--- D10: Very deep tree (20 levels) ---"

test_case "D10.1 create 20-level deep tree" \
  "$FUSE bash -c 'DEEP=$TEST_DIR/d10; for i in \$(seq 1 20); do DEEP=\$DEEP/l\$i; done; mkdir -p \$DEEP && [ -d \$DEEP ]'"

test_case "D10.2 create file at depth 20" \
  "$FUSE bash -c 'DEEP=$TEST_DIR/d10; for i in \$(seq 1 20); do DEEP=\$DEEP/l\$i; done; echo deep20 > \$DEEP/file.txt && [ -f \$DEEP/file.txt ]'"

test_case "D10.3 read file at depth 20" \
  "$FUSE bash -c 'DEEP=$TEST_DIR/d10; for i in \$(seq 1 20); do DEEP=\$DEEP/l\$i; done; [ \"\$(cat \$DEEP/file.txt)\" = \"deep20\" ]'"

test_case "D10.4 find file at depth 20" \
  "$FUSE bash -c 'find $TEST_DIR/d10 -name file.txt | grep -q l20'"

test_case "D10.5 rm -rf deep tree" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d10 && ! [ -d $TEST_DIR/d10 ]'"
echo ""

# ------------------------------------------------------------------
# D11: Special characters in names
# ------------------------------------------------------------------
echo "--- D11: Special characters in names ---"

test_case "D11.1 dir with spaces" \
  "$FUSE bash -c 'mkdir -p \"$TEST_DIR/d11/my dir\" && [ -d \"$TEST_DIR/d11/my dir\" ] && echo sp > \"$TEST_DIR/d11/my dir/file.txt\" && [ \"\$(cat \"$TEST_DIR/d11/my dir/file.txt\")\" = \"sp\" ]'"

test_case "D11.2 dir with dots" \
  "$FUSE bash -c 'mkdir -p $TEST_DIR/d11/dir.with.dots && [ -d $TEST_DIR/d11/dir.with.dots ]'"

test_case "D11.3 file with dashes" \
  "$FUSE bash -c 'echo dash > $TEST_DIR/d11/file-with-dashes.txt && [ -f $TEST_DIR/d11/file-with-dashes.txt ]'"

test_case "D11.4 cp -prf dir with spaces" \
  "$FUSE bash -c 'cp -prf \"$TEST_DIR/d11/my dir\" \"$TEST_DIR/d11/my dir copy\" && [ -d \"$TEST_DIR/d11/my dir copy\" ]'"

test_case "D11.5 find dir with spaces" \
  "$FUSE bash -c 'find $TEST_DIR/d11 -type d -name \"my dir\" | grep -q \"my dir\"'"

test_case "D11.6 cleanup D11" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d11'"
echo ""

# ------------------------------------------------------------------
# D12: Concurrent directory operations
# ------------------------------------------------------------------
echo "--- D12: Concurrent directory operations ---"

test_case "D12.1 concurrent mkdir (20 dirs)" \
  "$FUSE bash -c 'for i in \$(seq 1 20); do (mkdir -p $TEST_DIR/d12/dir_\$i/sub) & done; wait && [ \$(find $TEST_DIR/d12 -type d | wc -l) -eq 41 ]'"

test_case "D12.2 concurrent file create" \
  "$FUSE bash -c 'for i in \$(seq 1 20); do (echo \"data_\$i\" > $TEST_DIR/d12/dir_\$i/sub/file.txt) & done; wait && [ \$(find $TEST_DIR/d12 -name file.txt | wc -l) -eq 20 ]'"

test_case "D12.3 concurrent read" \
  "$FUSE bash -c 'for i in \$(seq 1 20); do ([ \"\$(cat $TEST_DIR/d12/dir_\$i/sub/file.txt)\" = \"data_\$i\" ]) & done; wait'"

test_case "D12.4 concurrent rm" \
  "$FUSE bash -c 'for i in \$(seq 1 10); do (rm -rf $TEST_DIR/d12/dir_\$i) & done; wait && [ \$(find $TEST_DIR/d12 -type d | wc -l) -eq 21 ]'"

test_case "D12.5 cleanup D12" \
  "$FUSE bash -c 'rm -rf $TEST_DIR/d12'"
echo ""

# ------------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------------
log "Final cleanup..."
$FUSE rm -rf "$TEST_DIR" 2>/dev/null || true

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Deep Directory Test Summary"
echo "============================================================"
echo "  Total:  $((PASS + FAIL))"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
if [ $FAIL -gt 0 ]; then
    echo ""
    echo "  Failed tests:"
    for t in "${FAILED_TESTS[@]}"; do
        echo "    - $t"
    done
fi
echo "============================================================"

exit $FAIL
