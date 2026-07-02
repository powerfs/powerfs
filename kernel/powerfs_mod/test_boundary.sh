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

test_large_file() {
    log "Testing large file write (1MB)..."
    
    local test_size=$((1024 * 1024))
    local test_content=$(head -c $test_size /dev/urandom | base64)
    
    echo -n "$test_content" > "$MOUNT_POINT/large_file.bin" || fail "Failed to write large file"
    
    local size=$(stat -c %s "$MOUNT_POINT/large_file.bin") || fail "Failed to get large file size"
    if [ "$size" -ne "${#test_content}" ]; then
        fail "Large file size mismatch: expected ${#test_content}, got $size"
    fi
    
    local read_content=$(cat "$MOUNT_POINT/large_file.bin") || fail "Failed to read large file"
    if [ "$read_content" != "$test_content" ]; then
        fail "Large file content mismatch"
    fi
    
    rm "$MOUNT_POINT/large_file.bin" || fail "Failed to remove large file"
    
    log "Large file test: PASSED ($(( ${#test_content} / 1024 )) KB)"
}

test_zero_size_file() {
    log "Testing zero-size file..."
    
    touch "$MOUNT_POINT/zero.txt" || fail "Failed to create zero-size file"
    
    local size=$(stat -c %s "$MOUNT_POINT/zero.txt") || fail "Failed to get zero-size file size"
    if [ "$size" -ne 0 ]; then
        fail "Zero-size file has non-zero size: $size"
    fi
    
    rm "$MOUNT_POINT/zero.txt" || fail "Failed to remove zero-size file"
    
    log "Zero-size file test: PASSED"
}

test_empty_directory() {
    log "Testing empty directory operations..."
    
    mkdir "$MOUNT_POINT/empty_test" || fail "Failed to create empty directory"
    
    local files=$(ls -a "$MOUNT_POINT/empty_test" | wc -l) || fail "Failed to list empty directory"
    if [ "$files" -ne 2 ]; then
        fail "Empty directory listing mismatch: expected 2, got $files"
    fi
    
    rmdir "$MOUNT_POINT/empty_test" || fail "Failed to remove empty directory"
    
    log "Empty directory test: PASSED"
}

test_max_filename() {
    log "Testing maximum filename length..."
    
    local max_name=$(printf 'x%.0s' {1..255})
    echo "test" > "$MOUNT_POINT/$max_name" || fail "Failed to create file with max filename"
    
    if [ ! -f "$MOUNT_POINT/$max_name" ]; then
        fail "Max filename file not found"
    fi
    
    rm "$MOUNT_POINT/$max_name" || fail "Failed to remove max filename file"
    
    log "Max filename test: PASSED (255 chars)"
}

test_root_directory() {
    log "Testing root directory operations..."
    
    echo "In root" > "$MOUNT_POINT/root_file.txt" || fail "Failed to create file in root"
    
    mkdir "$MOUNT_POINT/root_dir" || fail "Failed to create directory in root"
    
    local files=$(ls "$MOUNT_POINT" | grep -c "root") || fail "Failed to list root"
    if [ "$files" -ne 2 ]; then
        fail "Root listing mismatch"
    fi
    
    rm "$MOUNT_POINT/root_file.txt" || fail "Failed to remove root file"
    rmdir "$MOUNT_POINT/root_dir" || fail "Failed to remove root dir"
    
    log "Root directory test: PASSED"
}

test_nested_directory() {
    log "Testing deeply nested directories..."
    
    mkdir -p "$MOUNT_POINT/level1/level2/level3/level4/level5" || fail "Failed to create nested dirs"
    
    echo "Deeply nested" > "$MOUNT_POINT/level1/level2/level3/level4/level5/deep.txt" || fail "Failed to create file in deep dir"
    
    local content=$(cat "$MOUNT_POINT/level1/level2/level3/level4/level5/deep.txt") || fail "Failed to read deep file"
    if [ "$content" != "Deeply nested" ]; then
        fail "Deep file content mismatch"
    fi
    
    rm "$MOUNT_POINT/level1/level2/level3/level4/level5/deep.txt" || fail "Failed to remove deep file"
    rmdir "$MOUNT_POINT/level1/level2/level3/level4/level5" || fail "Failed to remove level5"
    rmdir "$MOUNT_POINT/level1/level2/level3/level4" || fail "Failed to remove level4"
    rmdir "$MOUNT_POINT/level1/level2/level3" || fail "Failed to remove level3"
    rmdir "$MOUNT_POINT/level1/level2" || fail "Failed to remove level2"
    rmdir "$MOUNT_POINT/level1" || fail "Failed to remove level1"
    
    log "Nested directory test: PASSED (5 levels)"
}

test_special_characters() {
    log "Testing filenames with special characters..."
    
    local special_names=("file with space.txt" "file@name.txt" "file#name.txt" "file$name.txt" "file&name.txt" "file(name).txt")
    
    for name in "${special_names[@]}"; do
        echo "test" > "$MOUNT_POINT/$name" || fail "Failed to create file with special chars: $name"
        if [ ! -f "$MOUNT_POINT/$name" ]; then
            fail "File with special chars not found: $name"
        fi
        rm "$MOUNT_POINT/$name" || fail "Failed to remove file with special chars: $name"
    done
    
    log "Special characters test: PASSED"
}

test_overwrite_file() {
    log "Testing file overwrite..."
    
    echo "First content" > "$MOUNT_POINT/overwrite.txt" || fail "Failed to create overwrite.txt"
    echo "Second content" > "$MOUNT_POINT/overwrite.txt" || fail "Failed to overwrite file"
    
    local content=$(cat "$MOUNT_POINT/overwrite.txt") || fail "Failed to read overwritten file"
    if [ "$content" != "Second content" ]; then
        fail "Overwrite content mismatch"
    fi
    
    rm "$MOUNT_POINT/overwrite.txt" || fail "Failed to remove overwrite.txt"
    
    log "File overwrite test: PASSED"
}

trap cleanup EXIT

log "=== PowerFS Boundary Test Suite ==="

log "Creating mount point: $MOUNT_POINT"
mkdir -p "$MOUNT_POINT"

log "Loading powerfs module..."
if lsmod | grep -q "powerfs"; then
    sudo rmmod powerfs || fail "Failed to unload existing powerfs module"
fi
sudo insmod "$MODULE_PATH" || fail "Failed to load powerfs module"

log "Mounting powerfs at $MOUNT_POINT..."
sudo mount -t powerfs none "$MOUNT_POINT" || fail "Failed to mount powerfs"

test_large_file
test_zero_size_file
test_empty_directory
test_max_filename
test_root_directory
test_nested_directory
test_special_characters
test_overwrite_file

log "=== All boundary tests PASSED ==="
