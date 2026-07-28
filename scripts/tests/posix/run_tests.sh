#!/bin/bash
# PowerFS POSIX Functionality Tests
# Tests basic POSIX file system operations

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

source "$PROJECT_ROOT/scripts/lib/common.sh"

setup_test_env
trap 'cleanup_test_env' EXIT

echo ""
echo "============================================================"
echo "  PowerFS POSIX Functionality Tests"
echo "============================================================"

build_binaries "release"
start_all_services

# Test 1: mkdir
test_mkdir() {
    test_start "mkdir operations"
    
    rm -rf "$MOUNT_DIR/mkdir_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/mkdir_test/a/b/c"
    mkdir "$MOUNT_DIR/mkdir_test/d"
    
    assert_dir_exists "$MOUNT_DIR/mkdir_test/a/b/c" "Nested dir created"
    assert_dir_exists "$MOUNT_DIR/mkdir_test/d" "Dir created"
    
    rm -rf "$MOUNT_DIR/mkdir_test"
    test_pass
}

# Test 2: rename
test_rename() {
    test_start "rename operations"
    
    rm -rf "$MOUNT_DIR/rename_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/rename_test"
    echo "test content" > "$MOUNT_DIR/rename_test/file.txt"
    mkdir "$MOUNT_DIR/rename_test/dir1"
    echo "dir content" > "$MOUNT_DIR/rename_test/dir1/nested.txt"
    
    mv "$MOUNT_DIR/rename_test/file.txt" "$MOUNT_DIR/rename_test/renamed.txt"
    mv "$MOUNT_DIR/rename_test/dir1" "$MOUNT_DIR/rename_test/dir2"
    
    assert_file_exists "$MOUNT_DIR/rename_test/renamed.txt" "File renamed"
    assert_file_not_exists "$MOUNT_DIR/rename_test/file.txt" "Old file gone"
    assert_dir_exists "$MOUNT_DIR/rename_test/dir2" "Dir renamed"
    assert_file_exists "$MOUNT_DIR/rename_test/dir2/nested.txt" "Nested file in new dir"
    
    local content
    content=$(cat "$MOUNT_DIR/rename_test/renamed.txt")
    assert_eq "test content" "$content" "Content preserved"
    
    rm -rf "$MOUNT_DIR/rename_test"
    test_pass
}

# Test 3: hard link
test_link() {
    test_start "hard link operations"
    
    rm -rf "$MOUNT_DIR/link_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/link_test"
    echo "hard link content" > "$MOUNT_DIR/link_test/original.txt"
    
    if ln "$MOUNT_DIR/link_test/original.txt" "$MOUNT_DIR/link_test/hardlink.txt" 2>/dev/null; then
        local content1 content2
        content1=$(cat "$MOUNT_DIR/link_test/original.txt")
        content2=$(cat "$MOUNT_DIR/link_test/hardlink.txt")
        assert_eq "$content1" "$content2" "Hard link content matches"
        
        echo "updated content" > "$MOUNT_DIR/link_test/hardlink.txt"
        local content3
        content3=$(cat "$MOUNT_DIR/link_test/original.txt")
        assert_eq "updated content" "$content3" "Update visible in original"
    else
        test_skip "hard link may not be fully supported"
        return 0
    fi
    
    rm -rf "$MOUNT_DIR/link_test"
    test_pass
}

# Test 4: symlink
test_symlink() {
    test_start "symlink operations"
    
    rm -rf "$MOUNT_DIR/symlink_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/symlink_test"
    echo "symlink target content" > "$MOUNT_DIR/symlink_test/target.txt"
    
    ln -s target.txt "$MOUNT_DIR/symlink_test/link.txt"
    
    if [ -L "$MOUNT_DIR/symlink_test/link.txt" ]; then
        local target
        target=$(readlink "$MOUNT_DIR/symlink_test/link.txt")
        assert_eq "target.txt" "$target" "Symlink target correct"
        
        local content
        content=$(cat "$MOUNT_DIR/symlink_test/link.txt")
        assert_eq "symlink target content" "$content" "Symlink content correct"
    else
        test_skip "symlink may not be fully supported"
        return 0
    fi
    
    rm -rf "$MOUNT_DIR/symlink_test"
    test_pass
}

# Test 5: file permissions
test_permissions() {
    test_start "file permissions"
    
    rm -rf "$MOUNT_DIR/perms_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/perms_test"
    echo "perm test" > "$MOUNT_DIR/perms_test/file.txt"
    
    chmod 600 "$MOUNT_DIR/perms_test/file.txt"
    local perms
    perms=$(stat -c "%a" "$MOUNT_DIR/perms_test/file.txt")
    assert_eq "600" "$perms" "Permissions set to 600"
    
    chmod 755 "$MOUNT_DIR/perms_test/file.txt"
    perms=$(stat -c "%a" "$MOUNT_DIR/perms_test/file.txt")
    assert_eq "755" "$perms" "Permissions changed to 755"
    
    rm -rf "$MOUNT_DIR/perms_test"
    test_pass
}

# Test 6: truncate
test_truncate() {
    test_start "truncate operations"
    
    rm -rf "$MOUNT_DIR/truncate_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/truncate_test"
    echo "this is a longer test string" > "$MOUNT_DIR/truncate_test/file.txt"
    
    truncate -s 10 "$MOUNT_DIR/truncate_test/file.txt"
    local size
    size=$(stat -c "%s" "$MOUNT_DIR/truncate_test/file.txt")
    assert_eq "10" "$size" "File truncated to 10 bytes"
    
    local content
    content=$(cat "$MOUNT_DIR/truncate_test/file.txt")
    assert_eq "this is a " "$content" "Truncated content correct"
    
    rm -rf "$MOUNT_DIR/truncate_test"
    test_pass
}

# Test 7: unlink
test_unlink() {
    test_start "unlink operations"
    
    rm -rf "$MOUNT_DIR/unlink_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/unlink_test"
    echo "to be deleted" > "$MOUNT_DIR/unlink_test/file.txt"
    
    assert_file_exists "$MOUNT_DIR/unlink_test/file.txt" "File created"
    
    rm "$MOUNT_DIR/unlink_test/file.txt"
    assert_file_not_exists "$MOUNT_DIR/unlink_test/file.txt" "File deleted"
    
    rm -rf "$MOUNT_DIR/unlink_test"
    test_pass
}

# Test 8: rmdir
test_rmdir() {
    test_start "rmdir operations"
    
    rm -rf "$MOUNT_DIR/rmdir_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/rmdir_test/a/b"
    mkdir "$MOUNT_DIR/rmdir_test/c"
    
    rmdir "$MOUNT_DIR/rmdir_test/a/b"
    rmdir "$MOUNT_DIR/rmdir_test/a"
    rmdir "$MOUNT_DIR/rmdir_test/c"
    
    assert_dir_not_exists "$MOUNT_DIR/rmdir_test/a" "Dir removed"
    assert_dir_not_exists "$MOUNT_DIR/rmdir_test/c" "Dir removed"
    
    rm -rf "$MOUNT_DIR/rmdir_test"
    test_pass
}

# Test 9: file read/write
test_read_write() {
    test_start "file read/write operations"
    
    rm -rf "$MOUNT_DIR/rw_test" 2>/dev/null || true
    
    mkdir -p "$MOUNT_DIR/rw_test"
    
    # Write
    echo "Hello PowerFS" > "$MOUNT_DIR/rw_test/test.txt"
    
    # Read
    local content
    content=$(cat "$MOUNT_DIR/rw_test/test.txt")
    assert_eq "Hello PowerFS" "$content" "Read-write works"
    
    # Overwrite
    echo "Updated content" > "$MOUNT_DIR/rw_test/test.txt"
    content=$(cat "$MOUNT_DIR/rw_test/test.txt")
    assert_eq "Updated content" "$content" "Overwrite works"
    
    # Append
    echo "Appended line" >> "$MOUNT_DIR/rw_test/test.txt"
    content=$(cat "$MOUNT_DIR/rw_test/test.txt")
    assert_eq "Updated content
Appended line" "$content" "Append works"
    
    rm -rf "$MOUNT_DIR/rw_test"
    test_pass
}

# Test 10: directory listing
test_directory_listing() {
    test_start "directory listing"
    
    rm -rf "$MOUNT_DIR/list_test" 2>/dev/null || true
    mkdir -p "$MOUNT_DIR/list_test"
    
    # Create test files
    for i in $(seq 1 5); do
        echo "file $i" > "$MOUNT_DIR/list_test/file_$i.txt"
    done
    
    # List and count
    local count
    count=$(ls "$MOUNT_DIR/list_test/" | wc -l)
    assert_eq "5" "$count" "Directory listing shows all files"
    
    # Check specific file
    assert_file_exists "$MOUNT_DIR/list_test/file_3.txt" "File exists in listing"
    
    # Delete one file
    rm "$MOUNT_DIR/list_test/file_2.txt"
    
    count=$(ls "$MOUNT_DIR/list_test/" | wc -l)
    assert_eq "4" "$count" "Listing updates after deletion"
    
    assert_file_not_exists "$MOUNT_DIR/list_test/file_2.txt" "Deleted file not in listing"
    
    rm -rf "$MOUNT_DIR/list_test"
    test_pass
}

# Run all tests
echo ""
echo "Running POSIX functionality tests..."
echo ""

test_mkdir
test_rename
test_link
test_symlink
test_permissions
test_truncate
test_unlink
test_rmdir
test_read_write
test_directory_listing

# Summary
echo ""
echo "============================================================"
echo "  POSIX Test Results"
echo "============================================================"
print_summary
