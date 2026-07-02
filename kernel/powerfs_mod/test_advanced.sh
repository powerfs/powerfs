#!/bin/bash

MOUNT_POINT="/mnt/powerfs_test"
MODULE_PATH="/home/portion/powerfs/kernel/powerfs_mod/powerfs.ko"

set -e

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

fail() {
    log "FAILED: $1"
    cleanup
    exit 1
}

cleanup() {
    log "Cleaning up..."
    if mount | grep -q "$MOUNT_POINT"; then
        log "Unmounting $MOUNT_POINT"
        sudo umount "$MOUNT_POINT" || true
    fi
    if lsmod | grep -q "powerfs"; then
        log "Unloading powerfs module"
        sudo rmmod powerfs || true
    fi
    rm -rf "$MOUNT_POINT" || true
}

test_symlink() {
    log "Testing symlink operations..."
    
    echo "Original content" > "$MOUNT_POINT/original.txt" || fail "Failed to create original.txt"
    
    ln -s original.txt "$MOUNT_POINT/link.txt" || fail "Failed to create symlink"
    
    local content=$(cat "$MOUNT_POINT/link.txt") || fail "Failed to read symlink"
    if [ "$content" != "Original content" ]; then
        fail "Symlink content mismatch"
    fi
    
    local target=$(readlink "$MOUNT_POINT/link.txt") || fail "Failed to readlink"
    if [ "$target" != "original.txt" ]; then
        fail "Symlink target mismatch"
    fi
    
    rm "$MOUNT_POINT/link.txt" || fail "Failed to remove symlink"
    rm "$MOUNT_POINT/original.txt" || fail "Failed to remove original"
    
    log "Symlink operations: PASSED"
}

test_hardlink() {
    log "Testing hardlink operations..."
    
    echo "Hardlink content" > "$MOUNT_POINT/hardlink_source.txt" || fail "Failed to create hardlink_source.txt"
    
    ln "$MOUNT_POINT/hardlink_source.txt" "$MOUNT_POINT/hardlink_target.txt" || fail "Failed to create hardlink"
    
    local content1=$(cat "$MOUNT_POINT/hardlink_source.txt") || fail "Failed to read source"
    local content2=$(cat "$MOUNT_POINT/hardlink_target.txt") || fail "Failed to read target"
    if [ "$content1" != "$content2" ]; then
        fail "Hardlink content mismatch"
    fi
    
    echo "Modified" > "$MOUNT_POINT/hardlink_target.txt" || fail "Failed to modify target"
    
    local content1=$(cat "$MOUNT_POINT/hardlink_source.txt") || fail "Failed to read source after modification"
    if [ "$content1" != "Modified" ]; then
        fail "Hardlink modification not reflected"
    fi
    
    local nlink=$(stat -c %h "$MOUNT_POINT/hardlink_source.txt") || fail "Failed to get nlink"
    if [ "$nlink" -ne 2 ]; then
        fail "Hardlink count mismatch: expected 2, got $nlink"
    fi
    
    rm "$MOUNT_POINT/hardlink_source.txt" || fail "Failed to remove source"
    
    if [ ! -f "$MOUNT_POINT/hardlink_target.txt" ]; then
        fail "Hardlink target removed when source removed"
    fi
    
    rm "$MOUNT_POINT/hardlink_target.txt" || fail "Failed to remove target"
    
    log "Hardlink operations: PASSED"
}

test_rename() {
    log "Testing rename operations..."
    
    echo "Rename test" > "$MOUNT_POINT/oldname.txt" || fail "Failed to create oldname.txt"
    mkdir "$MOUNT_POINT/dir1" || fail "Failed to create dir1"
    mkdir "$MOUNT_POINT/dir2" || fail "Failed to create dir2"
    
    mv "$MOUNT_POINT/oldname.txt" "$MOUNT_POINT/newname.txt" || fail "Failed to rename file"
    
    if [ ! -f "$MOUNT_POINT/newname.txt" ]; then
        fail "Renamed file not found"
    fi
    
    if [ -f "$MOUNT_POINT/oldname.txt" ]; then
        fail "Old file still exists"
    fi
    
    echo "In dir1" > "$MOUNT_POINT/dir1/file.txt" || fail "Failed to create file in dir1"
    mv "$MOUNT_POINT/dir1/file.txt" "$MOUNT_POINT/dir2/file.txt" || fail "Failed to move file between dirs"
    
    if [ ! -f "$MOUNT_POINT/dir2/file.txt" ]; then
        fail "File not moved to dir2"
    fi
    
    if [ -f "$MOUNT_POINT/dir1/file.txt" ]; then
        fail "File still exists in dir1"
    fi
    
    mv "$MOUNT_POINT/dir2" "$MOUNT_POINT/dir2_renamed" || fail "Failed to rename directory"
    
    if [ ! -d "$MOUNT_POINT/dir2_renamed" ]; then
        fail "Renamed directory not found"
    fi
    
    rm "$MOUNT_POINT/newname.txt" || fail "Failed to remove newname.txt"
    rm "$MOUNT_POINT/dir2_renamed/file.txt" || fail "Failed to remove file in renamed dir"
    rmdir "$MOUNT_POINT/dir2_renamed" || fail "Failed to remove renamed dir"
    rmdir "$MOUNT_POINT/dir1" || fail "Failed to remove dir1"
    
    log "Rename operations: PASSED"
}

test_setattr() {
    log "Testing setattr/truncate operations..."
    
    echo "1234567890" > "$MOUNT_POINT/trunc_test.txt" || fail "Failed to create trunc_test.txt"
    
    local size=$(stat -c %s "$MOUNT_POINT/trunc_test.txt") || fail "Failed to get size"
    if [ "$size" -ne 11 ]; then
        fail "Initial size mismatch: expected 11, got $size"
    fi
    
    truncate -s 5 "$MOUNT_POINT/trunc_test.txt" || fail "Failed to truncate"
    
    local size=$(stat -c %s "$MOUNT_POINT/trunc_test.txt") || fail "Failed to get size after truncate"
    if [ "$size" -ne 5 ]; then
        fail "Truncated size mismatch: expected 5, got $size"
    fi
    
    local content=$(cat "$MOUNT_POINT/trunc_test.txt") || fail "Failed to read truncated file"
    if [ "$content" != "12345" ]; then
        fail "Truncated content mismatch"
    fi
    
    chmod 755 "$MOUNT_POINT/trunc_test.txt" || fail "Failed to chmod"
    
    local mode=$(stat -c %a "$MOUNT_POINT/trunc_test.txt") || fail "Failed to get mode"
    if [ "$mode" != "755" ]; then
        fail "Mode mismatch: expected 755, got $mode"
    fi
    
    rm "$MOUNT_POINT/trunc_test.txt" || fail "Failed to remove trunc_test.txt"
    
    log "Setattr/truncate operations: PASSED"
}

test_mkdir_with_permissions() {
    log "Testing mkdir with permissions..."
    
    mkdir -m 700 "$MOUNT_POINT/perm_dir" || fail "Failed to create perm_dir"
    
    local mode=$(stat -c %a "$MOUNT_POINT/perm_dir") || fail "Failed to get dir mode"
    if [ "$mode" != "700" ]; then
        fail "Dir mode mismatch: expected 700, got $mode"
    fi
    
    rmdir "$MOUNT_POINT/perm_dir" || fail "Failed to remove perm_dir"
    
    log "Mkdir with permissions: PASSED"
}

trap cleanup EXIT

log "=== PowerFS Advanced Test Suite ==="

log "Creating mount point: $MOUNT_POINT"
mkdir -p "$MOUNT_POINT"

log "Loading powerfs module..."
if lsmod | grep -q "powerfs"; then
    sudo rmmod powerfs || fail "Failed to unload existing powerfs module"
fi
sudo insmod "$MODULE_PATH" || fail "Failed to load powerfs module"

log "Mounting powerfs at $MOUNT_POINT..."
sudo mount -t powerfs none "$MOUNT_POINT" || fail "Failed to mount powerfs"

test_symlink
test_hardlink
test_rename
test_setattr
test_mkdir_with_permissions

log "=== All advanced tests PASSED ==="
