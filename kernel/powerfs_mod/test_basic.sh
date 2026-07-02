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

test_file_create_read_write() {
    log "Testing file create, read, write..."
    
    echo "Hello PowerFS" > "$MOUNT_POINT/testfile.txt" || fail "Failed to create testfile.txt"
    
    local content=$(cat "$MOUNT_POINT/testfile.txt") || fail "Failed to read testfile.txt"
    if [ "$content" != "Hello PowerFS" ]; then
        fail "File content mismatch: expected 'Hello PowerFS', got '$content'"
    fi
    
    echo "Appended content" >> "$MOUNT_POINT/testfile.txt" || fail "Failed to append to testfile.txt"
    
    local content=$(cat "$MOUNT_POINT/testfile.txt") || fail "Failed to read testfile.txt after append"
    if ! echo "$content" | grep -q "Appended content"; then
        fail "Append failed"
    fi
    
    log "File create/read/write: PASSED"
}

test_directory_operations() {
    log "Testing directory operations..."
    
    mkdir "$MOUNT_POINT/testdir" || fail "Failed to create testdir"
    
    echo "Nested file" > "$MOUNT_POINT/testdir/nested.txt" || fail "Failed to create nested file"
    
    local files=$(ls "$MOUNT_POINT/testdir") || fail "Failed to list testdir"
    if [ "$files" != "nested.txt" ]; then
        fail "Directory listing mismatch"
    fi
    
    mkdir "$MOUNT_POINT/testdir/subdir" || fail "Failed to create subdir"
    
    local files=$(ls "$MOUNT_POINT/testdir") || fail "Failed to list testdir after subdir creation"
    if ! echo "$files" | grep -q "subdir"; then
        fail "Subdirectory not created"
    fi
    
    rmdir "$MOUNT_POINT/testdir/subdir" || fail "Failed to remove empty subdir"
    
    log "Directory operations: PASSED"
}

test_file_remove() {
    log "Testing file removal..."
    
    echo "To be deleted" > "$MOUNT_POINT/todelete.txt" || fail "Failed to create todelete.txt"
    
    rm "$MOUNT_POINT/todelete.txt" || fail "Failed to remove todelete.txt"
    
    if [ -f "$MOUNT_POINT/todelete.txt" ]; then
        fail "File was not removed"
    fi
    
    log "File removal: PASSED"
}

test_directory_remove() {
    log "Testing directory removal..."
    
    mkdir -p "$MOUNT_POINT/emptydir" || fail "Failed to create emptydir"
    rmdir "$MOUNT_POINT/emptydir" || fail "Failed to remove emptydir"
    
    mkdir -p "$MOUNT_POINT/nonemptydir" || fail "Failed to create nonemptydir"
    echo "content" > "$MOUNT_POINT/nonemptydir/file.txt" || fail "Failed to create file in nonemptydir"
    
    if rmdir "$MOUNT_POINT/nonemptydir" 2>/dev/null; then
        fail "Should not be able to remove non-empty directory"
    fi
    
    rm "$MOUNT_POINT/nonemptydir/file.txt" || fail "Failed to remove file in nonemptydir"
    rmdir "$MOUNT_POINT/nonemptydir" || fail "Failed to remove now-empty directory"
    
    log "Directory removal: PASSED"
}

test_stat() {
    log "Testing stat operations..."
    
    echo "Stat test" > "$MOUNT_POINT/statfile.txt" || fail "Failed to create statfile.txt"
    
    local size=$(stat -c %s "$MOUNT_POINT/statfile.txt") || fail "Failed to stat file"
    if [ "$size" -ne 10 ]; then
        fail "File size mismatch: expected 10, got $size"
    fi
    
    local mode=$(stat -c %a "$MOUNT_POINT/statfile.txt") || fail "Failed to stat file mode"
    if [ "$mode" != "644" ]; then
        fail "File mode mismatch: expected 644, got $mode"
    fi
    
    log "Stat operations: PASSED"
}

test_statfs() {
    log "Testing statfs..."
    
    local fstype=$(stat -f -c %T "$MOUNT_POINT") || fail "Failed to statfs"
    if [ "$fstype" != "powerfs" ]; then
        fail "Filesystem type mismatch: expected 'powerfs', got '$fstype'"
    fi
    
    log "Statfs: PASSED"
}

trap cleanup EXIT

log "=== PowerFS Basic Test Suite ==="

log "Creating mount point: $MOUNT_POINT"
mkdir -p "$MOUNT_POINT"

log "Loading powerfs module..."
if lsmod | grep -q "powerfs"; then
    sudo rmmod powerfs || fail "Failed to unload existing powerfs module"
fi
sudo insmod "$MODULE_PATH" || fail "Failed to load powerfs module"

log "Mounting powerfs at $MOUNT_POINT..."
sudo mount -t powerfs none "$MOUNT_POINT" || fail "Failed to mount powerfs"

test_file_create_read_write
test_directory_operations
test_file_remove
test_directory_remove
test_stat
test_statfs

log "=== All basic tests PASSED ==="
