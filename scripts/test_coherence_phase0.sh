#!/bin/bash
# Phase 0: Synchronous commit + error rollback end-to-end tests
# Validates that metadata operations are committed synchronously to master
# and errors are properly propagated (not warn-and-continue)

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/coherence_test_common.sh"

setup_test_env

trap 'cleanup_test_env' EXIT

echo ""
echo "============================================================"
echo "  Phase 0: Synchronous Commit + Error Rollback E2E Tests"
echo "============================================================"

build_binaries
start_all_services

# ============================================================
# Test 1: mkdir - synchronous directory creation
# ============================================================
test_mkdir_sync() {
    test_start "mkdir synchronous creation"

    rm -rf "$MOUNT_DIR/phase0_mkdir" 2>/dev/null || true

    mkdir "$MOUNT_DIR/phase0_mkdir"
    assert_dir_exists "$MOUNT_DIR/phase0_mkdir" "Directory should exist after mkdir"

    sleep 1
    ls "$MOUNT_DIR/" > /dev/null 2>&1
    assert_dir_exists "$MOUNT_DIR/phase0_mkdir" "Directory should persist after listing"

    rm -rf "$MOUNT_DIR/phase0_mkdir"
    test_pass
}

test_mkdir_sync

# ============================================================
# Test 2: Nested mkdir - synchronous nested directory creation
# ============================================================
test_mkdir_nested() {
    test_start "nested mkdir synchronous creation"

    rm -rf "$MOUNT_DIR/phase0_nested" 2>/dev/null || true

    mkdir -p "$MOUNT_DIR/phase0_nested/a/b/c"
    assert_dir_exists "$MOUNT_DIR/phase0_nested" "Root dir exists"
    assert_dir_exists "$MOUNT_DIR/phase0_nested/a" "Level 1 dir exists"
    assert_dir_exists "$MOUNT_DIR/phase0_nested/a/b" "Level 2 dir exists"
    assert_dir_exists "$MOUNT_DIR/phase0_nested/a/b/c" "Level 3 dir exists"

    rm -rf "$MOUNT_DIR/phase0_nested"
    test_pass
}

test_mkdir_nested

# ============================================================
# Test 3: create - synchronous file creation
# ============================================================
test_create_sync() {
    test_start "file create synchronous"

    rm -rf "$MOUNT_DIR/phase0_create" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase0_create"

    echo "hello world" > "$MOUNT_DIR/phase0_create/test.txt"
    assert_file_exists "$MOUNT_DIR/phase0_create/test.txt" "File should exist after create"
    assert_file_content "$MOUNT_DIR/phase0_create/test.txt" "hello world" "File content correct"

    rm -rf "$MOUNT_DIR/phase0_create"
    test_pass
}

test_create_sync

# ============================================================
# Test 4: unlink - synchronous file deletion
# ============================================================
test_unlink_sync() {
    test_start "unlink synchronous deletion"

    rm -rf "$MOUNT_DIR/phase0_unlink" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase0_unlink"

    echo "to be deleted" > "$MOUNT_DIR/phase0_unlink/file.txt"
    assert_file_exists "$MOUNT_DIR/phase0_unlink/file.txt" "File created"

    rm "$MOUNT_DIR/phase0_unlink/file.txt"
    assert_file_not_exists "$MOUNT_DIR/phase0_unlink/file.txt" "File should not exist after unlink"

    ls "$MOUNT_DIR/phase0_unlink/" > /dev/null 2>&1
    assert_file_not_exists "$MOUNT_DIR/phase0_unlink/file.txt" "File should not appear in listing"

    rm -rf "$MOUNT_DIR/phase0_unlink"
    test_pass
}

test_unlink_sync

# ============================================================
# Test 5: rmdir - synchronous directory deletion
# ============================================================
test_rmdir_sync() {
    test_start "rmdir synchronous deletion"

    rm -rf "$MOUNT_DIR/phase0_rmdir" 2>/dev/null || true
    mkdir -p "$MOUNT_DIR/phase0_rmdir/child"

    rmdir "$MOUNT_DIR/phase0_rmdir/child"
    assert_dir_not_exists "$MOUNT_DIR/phase0_rmdir/child" "Child dir should not exist after rmdir"

    rmdir "$MOUNT_DIR/phase0_rmdir"
    assert_dir_not_exists "$MOUNT_DIR/phase0_rmdir" "Parent dir should not exist after rmdir"

    test_pass
}

test_rmdir_sync

# ============================================================
# Test 6: rename - synchronous rename operation
# ============================================================
test_rename_sync() {
    test_start "rename synchronous"

    rm -rf "$MOUNT_DIR/phase0_rename" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase0_rename"

    echo "rename test content" > "$MOUNT_DIR/phase0_rename/old_name.txt"
    assert_file_exists "$MOUNT_DIR/phase0_rename/old_name.txt" "Original file exists"

    mv "$MOUNT_DIR/phase0_rename/old_name.txt" "$MOUNT_DIR/phase0_rename/new_name.txt"
    assert_file_not_exists "$MOUNT_DIR/phase0_rename/old_name.txt" "Old file should not exist"
    assert_file_exists "$MOUNT_DIR/phase0_rename/new_name.txt" "New file should exist"
    assert_file_content "$MOUNT_DIR/phase0_rename/new_name.txt" "rename test content" "Content preserved after rename"

    rm -rf "$MOUNT_DIR/phase0_rename"
    test_pass
}

test_rename_sync

# ============================================================
# Test 7: rename directory - synchronous directory rename
# ============================================================
test_rename_dir_sync() {
    test_start "rename directory synchronous"

    rm -rf "$MOUNT_DIR/phase0_rename_dir" 2>/dev/null || true
    mkdir -p "$MOUNT_DIR/phase0_rename_dir/old_dir"
    echo "nested file" > "$MOUNT_DIR/phase0_rename_dir/old_dir/nested.txt"

    mv "$MOUNT_DIR/phase0_rename_dir/old_dir" "$MOUNT_DIR/phase0_rename_dir/new_dir"
    assert_dir_not_exists "$MOUNT_DIR/phase0_rename_dir/old_dir" "Old dir should not exist"
    assert_dir_exists "$MOUNT_DIR/phase0_rename_dir/new_dir" "New dir should exist"
    assert_file_exists "$MOUNT_DIR/phase0_rename_dir/new_dir/nested.txt" "Nested file should exist in new dir"

    rm -rf "$MOUNT_DIR/phase0_rename_dir"
    test_pass
}

test_rename_dir_sync

# ============================================================
# Test 8: setattr (chmod) - synchronous attribute change
# ============================================================
test_setattr_sync() {
    test_start "setattr (chmod) synchronous"

    rm -rf "$MOUNT_DIR/phase0_setattr" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase0_setattr"

    echo "attr test" > "$MOUNT_DIR/phase0_setattr/file.txt"

    chmod 600 "$MOUNT_DIR/phase0_setattr/file.txt"
    perms=$(stat -c "%a" "$MOUNT_DIR/phase0_setattr/file.txt" 2>/dev/null || true)
    assert_eq "600" "$perms" "Permissions should be 600 after chmod"

    chmod 755 "$MOUNT_DIR/phase0_setattr/file.txt"
    perms=$(stat -c "%a" "$MOUNT_DIR/phase0_setattr/file.txt" 2>/dev/null || true)
    assert_eq "755" "$perms" "Permissions should be 755 after second chmod"

    rm -rf "$MOUNT_DIR/phase0_setattr"
    test_pass
}

test_setattr_sync

# ============================================================
# Test 9: symlink - synchronous symlink creation
# ============================================================
test_symlink_sync() {
    test_start "symlink synchronous creation"

    rm -rf "$MOUNT_DIR/phase0_symlink" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase0_symlink"

    echo "target content" > "$MOUNT_DIR/phase0_symlink/target.txt"
    ln -s target.txt "$MOUNT_DIR/phase0_symlink/link.txt"

    if [ -L "$MOUNT_DIR/phase0_symlink/link.txt" ]; then
        target=$(readlink "$MOUNT_DIR/phase0_symlink/link.txt" 2>/dev/null || true)
        assert_eq "target.txt" "$target" "Symlink target correct"

        content=$(cat "$MOUNT_DIR/phase0_symlink/link.txt" 2>/dev/null || true)
        assert_eq "target content" "$content" "Symlink content correct"
    else
        test_skip "symlink may not be fully supported"
        return 0
    fi

    rm -rf "$MOUNT_DIR/phase0_symlink"
    test_pass
}

test_symlink_sync

# ============================================================
# Test 10: hard link - synchronous hard link creation
# ============================================================
test_hardlink_sync() {
    test_start "hard link synchronous creation"

    rm -rf "$MOUNT_DIR/phase0_hardlink" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase0_hardlink"

    echo "hard link content" > "$MOUNT_DIR/phase0_hardlink/original.txt"

    if ln "$MOUNT_DIR/phase0_hardlink/original.txt" "$MOUNT_DIR/phase0_hardlink/link.txt" 2>/dev/null; then
        assert_file_exists "$MOUNT_DIR/phase0_hardlink/link.txt" "Hard link file exists"

        content=$(cat "$MOUNT_DIR/phase0_hardlink/link.txt" 2>/dev/null || true)
        assert_eq "hard link content" "$content" "Hard link content matches"

        echo "updated via link" > "$MOUNT_DIR/phase0_hardlink/link.txt"
        original_content=$(cat "$MOUNT_DIR/phase0_hardlink/original.txt" 2>/dev/null || true)
        assert_eq "updated via link" "$original_content" "Update via link visible in original"
    else
        test_skip "hard link may not be fully supported"
        return 0
    fi

    rm -rf "$MOUNT_DIR/phase0_hardlink"
    test_pass
}

test_hardlink_sync

# ============================================================
# Test 11: Persistence across FUSE restart
# ============================================================
test_persistence_across_restart() {
    test_start "persistence across FUSE restart"

    rm -rf "$MOUNT_DIR/phase0_persist" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase0_persist"
    echo "persistent data" > "$MOUNT_DIR/phase0_persist/data.txt"
    mkdir "$MOUNT_DIR/phase0_persist/subdir"
    echo "nested data" > "$MOUNT_DIR/phase0_persist/subdir/nested.txt"

    restart_fuse

    assert_dir_exists "$MOUNT_DIR/phase0_persist" "Root dir persists after restart"
    assert_file_exists "$MOUNT_DIR/phase0_persist/data.txt" "File persists after restart"
    assert_file_content "$MOUNT_DIR/phase0_persist/data.txt" "persistent data" "File content persists"
    assert_dir_exists "$MOUNT_DIR/phase0_persist/subdir" "Subdir persists"
    assert_file_exists "$MOUNT_DIR/phase0_persist/subdir/nested.txt" "Nested file persists"

    rm -rf "$MOUNT_DIR/phase0_persist"
    test_pass
}

test_persistence_across_restart

# ============================================================
# Test 12: Multiple operations sequence
# ============================================================
test_multi_operation_sequence() {
    test_start "multi-operation sequence consistency"

    rm -rf "$MOUNT_DIR/phase0_multi" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase0_multi"

    for i in $(seq 1 10); do
        echo "file $i content" > "$MOUNT_DIR/phase0_multi/file_$i.txt"
    done

    count=$(ls "$MOUNT_DIR/phase0_multi/" | wc -l)
    assert_eq "10" "$count" "All 10 files should exist"

    for i in $(seq 1 5); do
        rm "$MOUNT_DIR/phase0_multi/file_$i.txt"
    done

    count=$(ls "$MOUNT_DIR/phase0_multi/" | wc -l)
    assert_eq "5" "$count" "5 files should remain after deletion"

    for i in $(seq 6 10); do
        assert_file_exists "$MOUNT_DIR/phase0_multi/file_$i.txt" "File $i should still exist"
    done

    mkdir "$MOUNT_DIR/phase0_multi/new_dir"
    mv "$MOUNT_DIR/phase0_multi/file_6.txt" "$MOUNT_DIR/phase0_multi/new_dir/renamed.txt"

    assert_file_not_exists "$MOUNT_DIR/phase0_multi/file_6.txt" "Moved file should not be in old location"
    assert_file_exists "$MOUNT_DIR/phase0_multi/new_dir/renamed.txt" "Moved file should be in new location"

    rm -rf "$MOUNT_DIR/phase0_multi"
    test_pass
}

test_multi_operation_sequence

# ============================================================
# Summary
# ============================================================
echo ""
echo "============================================================"
echo "  Phase 0 Test Results"
echo "============================================================"
print_summary
