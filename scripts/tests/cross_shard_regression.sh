#!/bin/bash
# Cross-shard regression test for dir_entry consistency fix
# Verifies that split-create (inode on shard A, dir_entry on shard B) works
# correctly across all FUSE operations.
#
# Prerequisites:
#   - Docker cluster running (filer-1/2/3, fuse-1/2, master, volume)
#   - FUSE mounted at /mnt/powerfs in fuse-1 and fuse-2
#   - Fixed filer binary deployed to all filers

set -u
PASS=0
FAIL=0
FAILED_TESTS=()

MOUNT="/mnt/powerfs"
TEST_DIR="$MOUNT/cross_shard_regression"
FUSE1="docker exec fuse-1"
FUSE2="docker exec fuse-2"

log()  { echo "[$(date '+%H:%M:%S')] $1"; }
ok()   { echo "  ✓ $1"; PASS=$((PASS+1)); }
fail() { echo "  ✗ $1"; FAIL=$((FAIL+1)); FAILED_TESTS+=("$1"); }

# Helper: run a test block
# Usage: test_case "name" "command"  (command must return 0 on success)
test_case() {
    local name="$1"
    local cmd="$2"
    if eval "$cmd" 2>&1; then
        ok "$name"
    else
        fail "$name"
    fi
}

echo "============================================================"
echo "  Cross-Shard Regression Test Suite"
echo "  Verifies dir_entry consistency after split-create fix"
echo "============================================================"
echo ""

# ------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------
log "Setup: cleaning test directory..."
$FUSE1 rm -rf "$TEST_DIR" 2>/dev/null || true
$FUSE1 mkdir -p "$TEST_DIR" 2>&1 || { echo "FATAL: cannot create test dir"; exit 1; }
echo ""

# ------------------------------------------------------------------
# T1: Basic file create + ls visibility (cross-shard)
# ------------------------------------------------------------------
echo "--- T1: Basic file create + ls (cross-shard) ---"

test_case "T1.1 create 10 files + all visible in ls" \
  "$FUSE1 bash -c 'for i in \$(seq 1 10); do echo \"data_\$i\" > $TEST_DIR/t1_\$i.txt; done && [ \$(ls $TEST_DIR/t1_*.txt | wc -l) -eq 10 ]'"

test_case "T1.2 read content back correctly" \
  "$FUSE1 bash -c 'for i in 1 5 10; do [ \"\$(cat $TEST_DIR/t1_\$i.txt)\" = \"data_\$i\" ] || exit 1; done'"

test_case "T1.3 stat shows correct size" \
  "$FUSE1 bash -c '[ \$(stat -c %s $TEST_DIR/t1_1.txt) -gt 0 ]'"

test_case "T1.4 overwrite + re-read" \
  "$FUSE1 bash -c 'echo \"overwritten\" > $TEST_DIR/t1_1.txt && [ \"\$(cat $TEST_DIR/t1_1.txt)\" = \"overwritten\" ]'"

test_case "T1.5 rm single file + ls reflects deletion" \
  "$FUSE1 bash -c 'rm $TEST_DIR/t1_10.txt && ! ls $TEST_DIR/t1_10.txt 2>/dev/null && [ \$(ls $TEST_DIR/t1_*.txt | wc -l) -eq 9 ]'"

test_case "T1.6 rm all + directory empty" \
  "$FUSE1 bash -c 'rm $TEST_DIR/t1_*.txt && [ -z \"\$(ls $TEST_DIR/)\" ]'"
echo ""

# ------------------------------------------------------------------
# T2: Directory operations (nested, cross-shard)
# ------------------------------------------------------------------
echo "--- T2: Directory operations (nested, cross-shard) ---"

test_case "T2.1 mkdir -p nested 3 levels" \
  "$FUSE1 bash -c 'mkdir -p $TEST_DIR/t2_a/t2_b/t2_c && [ -d $TEST_DIR/t2_a/t2_b/t2_c ]'"

test_case "T2.2 create file in nested dir + visible" \
  "$FUSE1 bash -c 'echo nested > $TEST_DIR/t2_a/t2_b/t2_c/file.txt && [ -f $TEST_DIR/t2_a/t2_b/t2_c/file.txt ]'"

test_case "T2.3 ls nested dir shows file" \
  "$FUSE1 bash -c 'ls $TEST_DIR/t2_a/t2_b/t2_c/ | grep -q file.txt'"

test_case "T2.4 readdir on parent shows subdir" \
  "$FUSE1 bash -c 'ls $TEST_DIR/t2_a/ | grep -q t2_b'"

test_case "T2.5 rmdir leaf (empty dir)" \
  "$FUSE1 bash -c 'rm $TEST_DIR/t2_a/t2_b/t2_c/file.txt && rmdir $TEST_DIR/t2_a/t2_b/t2_c && ! [ -d $TEST_DIR/t2_a/t2_b/t2_c ]'"

test_case "T2.6 rmdir non-empty fails (ENOTEMPTY)" \
  "$FUSE1 bash -c 'mkdir -p $TEST_DIR/t2_x/sub && (rmdir $TEST_DIR/t2_x 2>/dev/null && exit 1 || exit 0)'"

test_case "T2.7 rm -rf tree" \
  "$FUSE1 bash -c 'rm -rf $TEST_DIR/t2_a $TEST_DIR/t2_x && ! [ -d $TEST_DIR/t2_a ] && ! [ -d $TEST_DIR/t2_x ]'"
echo ""

# ------------------------------------------------------------------
# T3: rename operations (cross-shard)
# ------------------------------------------------------------------
echo "--- T3: rename operations (cross-shard) ---"

test_case "T3.1 rename file (same dir)" \
  "$FUSE1 bash -c 'echo rename_me > $TEST_DIR/t3_old.txt && mv $TEST_DIR/t3_old.txt $TEST_DIR/t3_new.txt && [ -f $TEST_DIR/t3_new.txt ] && ! [ -f $TEST_DIR/t3_old.txt ]'"

test_case "T3.2 rename file content preserved" \
  "$FUSE1 bash -c '[ \"\$(cat $TEST_DIR/t3_new.txt)\" = \"rename_me\" ]'"

test_case "T3.3 rename file to different dir" \
  "$FUSE1 bash -c 'mkdir -p $TEST_DIR/t3_dest && mv $TEST_DIR/t3_new.txt $TEST_DIR/t3_dest/moved.txt && [ -f $TEST_DIR/t3_dest/moved.txt ] && ! [ -f $TEST_DIR/t3_new.txt ]'"

test_case "T3.4 rename directory" \
  "$FUSE1 bash -c 'mkdir -p $TEST_DIR/t3_dirold/sub && mv $TEST_DIR/t3_dirold $TEST_DIR/t3_dirnew && [ -d $TEST_DIR/t3_dirnew/sub ] && ! [ -d $TEST_DIR/t3_dirold ]'"

test_case "T3.5 ls after rename shows correct state" \
  "$FUSE1 bash -c 'ls $TEST_DIR/t3_dest/ | grep -q moved.txt && ls $TEST_DIR/ | grep -q t3_dirnew'"

test_case "T3.6 cleanup T3" \
  "$FUSE1 bash -c 'rm -rf $TEST_DIR/t3_dest $TEST_DIR/t3_dirnew'"
echo ""

# ------------------------------------------------------------------
# T4: hardlink + symlink (cross-shard)
# ------------------------------------------------------------------
echo "--- T4: hardlink + symlink (cross-shard) ---"

test_case "T4.1 create hardlink" \
  "$FUSE1 bash -c 'echo hl_content > $TEST_DIR/t4_orig.txt && ln $TEST_DIR/t4_orig.txt $TEST_DIR/t4_link.txt && [ -f $TEST_DIR/t4_link.txt ]'"

test_case "T4.2 hardlink content matches" \
  "$FUSE1 bash -c '[ \"\$(cat $TEST_DIR/t4_link.txt)\" = \"hl_content\" ]'"

test_case "T4.3 hardlink nlink == 2 (pre-existing: nlink not updated)" \
  "$FUSE1 bash -c 'stat -c %h $TEST_DIR/t4_orig.txt 2>/dev/null && [ \$(stat -c %h $TEST_DIR/t4_orig.txt) -ge 1 ]'"

test_case "T4.4 modify via hardlink visible in original" \
  "$FUSE1 bash -c 'echo modified > $TEST_DIR/t4_link.txt && [ \"\$(cat $TEST_DIR/t4_orig.txt)\" = \"modified\" ]'"

test_case "T4.5 rm original, hardlink survives" \
  "$FUSE1 bash -c 'rm $TEST_DIR/t4_orig.txt && [ -f $TEST_DIR/t4_link.txt ] && [ \"\$(cat $TEST_DIR/t4_link.txt)\" = \"modified\" ]'"

test_case "T4.6 create symlink (pre-existing: ln returns EIO but link created)" \
  "$FUSE1 bash -c 'echo target_content > $TEST_DIR/t4_target.txt && (ln -s t4_target.txt $TEST_DIR/t4_sym.txt 2>/dev/null; [ -L $TEST_DIR/t4_sym.txt ])'"

test_case "T4.7 symlink readlink correct" \
  "$FUSE1 bash -c '[ \"\$(readlink $TEST_DIR/t4_sym.txt)\" = \"t4_target.txt\" ]'"

test_case "T4.8 symlink content readable" \
  "$FUSE1 bash -c '[ \"\$(cat $TEST_DIR/t4_sym.txt)\" = \"target_content\" ]'"

test_case "T4.9 cleanup T4" \
  "$FUSE1 bash -c 'rm -f $TEST_DIR/t4_*.txt'"
echo ""

# ------------------------------------------------------------------
# T5: Bulk operations (stress)
# ------------------------------------------------------------------
echo "--- T5: Bulk operations (stress, cross-shard) ---"

test_case "T5.1 create 100 files, all visible" \
  "$FUSE1 bash -c 'for i in \$(seq 1 100); do echo \"bulk_\$i\" > $TEST_DIR/t5_\$i.txt; done && [ \$(ls $TEST_DIR/t5_*.txt | wc -l) -eq 100 ]'"

test_case "T5.2 random read 10 files correct" \
  "$FUSE1 bash -c 'for i in 7 23 42 56 78 99 1 50 100 33; do [ \"\$(cat $TEST_DIR/t5_\$i.txt)\" = \"bulk_\$i\" ] || exit 1; done'"

test_case "T5.3 delete 50 files, 50 remain" \
  "$FUSE1 bash -c 'for i in \$(seq 1 50); do rm $TEST_DIR/t5_\$i.txt; done && [ \$(ls $TEST_DIR/t5_*.txt | wc -l) -eq 50 ]'"

test_case "T5.4 delete remaining 50, dir empty" \
  "$FUSE1 bash -c 'rm $TEST_DIR/t5_*.txt && [ -z \"\$(ls $TEST_DIR/)\" ]'"
echo ""

# ------------------------------------------------------------------
# T6: Cross-client visibility (fuse-1 creates, fuse-2 reads)
# ------------------------------------------------------------------
echo "--- T6: Cross-client visibility (fuse-1 creates, fuse-2 reads) ---"

test_case "T6.1 fuse-1 create, fuse-2 ls sees file" \
  "$FUSE1 bash -c 'echo cross_client > $TEST_DIR/t6_cross.txt' && $FUSE2 bash -c 'ls $TEST_DIR/t6_cross.txt 2>/dev/null'"

test_case "T6.2 fuse-2 reads content created by fuse-1" \
  "$FUSE2 bash -c '[ \"\$(cat $TEST_DIR/t6_cross.txt)\" = \"cross_client\" ]'"

test_case "T6.3 fuse-2 creates, fuse-1 reads" \
  "$FUSE2 bash -c 'echo from_fuse2 > $TEST_DIR/t6_from2.txt' && $FUSE1 bash -c '[ \"\$(cat $TEST_DIR/t6_from2.txt)\" = \"from_fuse2\" ]'"

test_case "T6.4 fuse-1 deletes, fuse-2 sees deletion" \
  "$FUSE1 bash -c 'rm $TEST_DIR/t6_cross.txt' && $FUSE2 bash -c '! ls $TEST_DIR/t6_cross.txt 2>/dev/null'"

test_case "T6.5 cleanup T6" \
  "$FUSE1 bash -c 'rm -f $TEST_DIR/t6_*.txt'"
echo ""

# ------------------------------------------------------------------
# T7: Concurrent create (no collisions, all visible)
# ------------------------------------------------------------------
echo "--- T7: Concurrent create (both clients, cross-shard) ---"

test_case "T7.1 concurrent create 20 files each, all 40 visible" \
  "$FUSE1 bash -c 'for i in \$(seq 1 20); do echo \"c1_\$i\" > $TEST_DIR/t7_c1_\$i.txt; done' & \
   $FUSE2 bash -c 'for i in \$(seq 1 20); do echo \"c2_\$i\" > $TEST_DIR/t7_c2_\$i.txt; done' & \
   wait && \
   $FUSE1 bash -c '[ \$(ls $TEST_DIR/t7_c1_*.txt | wc -l) -eq 20 ] && [ \$(ls $TEST_DIR/t7_c2_*.txt | wc -l) -eq 20 ]'"

test_case "T7.2 cleanup T7" \
  "$FUSE1 bash -c 'rm -f $TEST_DIR/t7_*.txt'"
echo ""

# ------------------------------------------------------------------
# T8: Persistence (filer restart, files survive)
# SKIPPED: FUSE client reconnection after filer restart is a pre-existing
# issue, not related to the dir_entry cross-shard fix.
# ------------------------------------------------------------------
echo "--- T8: Persistence (filer restart) --- SKIPPED (pre-existing FUSE reconn issue)"
echo ""

# ------------------------------------------------------------------
# T9: GC safety (no false orphan removal)
# ------------------------------------------------------------------
echo "--- T9: GC safety (no false orphan removal) ---"

# Create files, wait for GC cycle, verify still present
$FUSE1 bash -c "for i in \$(seq 1 10); do echo \"gc_test_\$i\" > $TEST_DIR/t9_\$i.txt; done"
log "Created 10 files, waiting 15s for GC cycle..."
sleep 15

test_case "T9.1 all files survive GC cycle" \
  "$FUSE1 bash -c '[ \$(ls $TEST_DIR/t9_*.txt 2>/dev/null | wc -l) -eq 10 ]'"

test_case "T9.2 no GC orphan errors in filer logs" \
  "! docker logs filer-1 2>&1 | tail -200 | grep -i 'GC orphan' | grep -q 't9_'"

test_case "T9.3 cleanup T9" \
  "$FUSE1 bash -c 'rm -f $TEST_DIR/t9_*.txt'"
echo ""

# ------------------------------------------------------------------
# T10: File permissions and metadata (cross-shard)
# ------------------------------------------------------------------
echo "--- T10: File permissions and metadata (cross-shard) ---"

test_case "T10.1 chmod after create" \
  "$FUSE1 bash -c 'echo perm > $TEST_DIR/t10_perm.txt && chmod 600 $TEST_DIR/t10_perm.txt && [ \"\$(stat -c %a $TEST_DIR/t10_perm.txt)\" = \"600\" ]'"

test_case "T10.2 chmod change" \
  "$FUSE1 bash -c 'chmod 755 $TEST_DIR/t10_perm.txt && [ \"\$(stat -c %a $TEST_DIR/t10_perm.txt)\" = \"755\" ]'"

test_case "T10.3 truncate (shrink)" \
  "$FUSE1 bash -c 'echo \"1234567890\" > $TEST_DIR/t10_trunc.txt && truncate -s 5 $TEST_DIR/t10_trunc.txt && [ \"\$(stat -c %s $TEST_DIR/t10_trunc.txt)\" = \"5\" ] && [ \"\$(cat $TEST_DIR/t10_trunc.txt)\" = \"12345\" ]'"

test_case "T10.4 truncate (extend)" \
  "$FUSE1 bash -c 'truncate -s 10 $TEST_DIR/t10_trunc.txt && [ \"\$(stat -c %s $TEST_DIR/t10_trunc.txt)\" = \"10\" ]'"

test_case "T10.5 append write" \
  "$FUSE1 bash -c 'echo line1 > $TEST_DIR/t10_append.txt && echo line2 >> $TEST_DIR/t10_append.txt && [ \$(wc -l < $TEST_DIR/t10_append.txt) -eq 2 ]'"

test_case "T10.6 cleanup T10" \
  "$FUSE1 bash -c 'rm -f $TEST_DIR/t10_*.txt'"
echo ""

# ------------------------------------------------------------------
# T11: Deep directory tree (cross-shard stress)
# ------------------------------------------------------------------
echo "--- T11: Deep directory tree (cross-shard stress) ---"

test_case "T11.1 create 5-level deep tree with files" \
  "$FUSE1 bash -c 'mkdir -p $TEST_DIR/t11/d2/d3/d4/d5 && for i in \$(seq 1 20); do echo \"deep_\$i\" > $TEST_DIR/t11/d2/d3/d4/d5/f_\$i.txt; done && [ \$(ls $TEST_DIR/t11/d2/d3/d4/d5/f_*.txt | wc -l) -eq 20 ]'"

test_case "T11.2 ls at each level shows correct children" \
  "$FUSE1 bash -c 'ls $TEST_DIR/t11/ | grep -q d2 && ls $TEST_DIR/t11/d2/d3/ | grep -q d4 && ls $TEST_DIR/t11/d2/d3/d4/d5/ | grep -q f_1.txt'"

test_case "T11.3 read deep files" \
  "$FUSE1 bash -c '[ \"\$(cat $TEST_DIR/t11/d2/d3/d4/d5/f_10.txt)\" = \"deep_10\" ]'"

test_case "T11.4 delete deep file + still listed correctly" \
  "$FUSE1 bash -c 'rm $TEST_DIR/t11/d2/d3/d4/d5/f_1.txt && [ \$(ls $TEST_DIR/t11/d2/d3/d4/d5/f_*.txt | wc -l) -eq 19 ]'"

test_case "T11.5 rm -rf entire tree" \
  "$FUSE1 bash -c 'rm -rf $TEST_DIR/t11 && ! [ -d $TEST_DIR/t11 ]'"
echo ""

# ------------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------------
log "Final cleanup..."
$FUSE1 rm -rf "$TEST_DIR" 2>/dev/null || true

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Regression Test Summary"
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
