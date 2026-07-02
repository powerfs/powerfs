#!/bin/bash

MOUNT_POINT="/mnt/powerfs_test"
MODULE_PATH="/home/portion/powerfs/kernel/powerfs_mod/powerfs.ko"

NUM_THREADS=4
NUM_OPERATIONS=100

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

test_concurrent_writes() {
    log "Testing concurrent writes to single file..."
    
    rm -f "$MOUNT_POINT/concurrency_test.txt" || true
    
    local pids=()
    
    for i in $(seq 1 $NUM_THREADS); do
        (
            for j in $(seq 1 $NUM_OPERATIONS); do
                echo "Thread $i, Operation $j" >> "$MOUNT_POINT/concurrency_test.txt"
            done
        ) &
        pids+=($!)
    done
    
    for pid in "${pids[@]}"; do
        wait "$pid" || fail "Thread $pid failed"
    done
    
    local line_count=$(wc -l < "$MOUNT_POINT/concurrency_test.txt") || fail "Failed to count lines"
    local expected=$((NUM_THREADS * NUM_OPERATIONS))
    
    if [ "$line_count" -ne "$expected" ]; then
        fail "Line count mismatch: expected $expected, got $line_count"
    fi
    
    log "Concurrent writes: PASSED ($line_count lines written)"
}

test_concurrent_reads() {
    log "Testing concurrent reads from single file..."
    
    echo "Test content for concurrent reads" > "$MOUNT_POINT/read_test.txt" || fail "Failed to create read_test.txt"
    
    local pids=()
    
    for i in $(seq 1 $NUM_THREADS); do
        (
            for j in $(seq 1 $NUM_OPERATIONS); do
                local content=$(cat "$MOUNT_POINT/read_test.txt")
                if [ "$content" != "Test content for concurrent reads" ]; then
                    echo "Read content mismatch in thread $i" >&2
                    exit 1
                fi
            done
        ) &
        pids+=($!)
    done
    
    for pid in "${pids[@]}"; do
        wait "$pid" || fail "Thread $pid failed during read"
    done
    
    rm "$MOUNT_POINT/read_test.txt" || fail "Failed to remove read_test.txt"
    
    log "Concurrent reads: PASSED"
}

test_concurrent_file_creation() {
    log "Testing concurrent file creation..."
    
    local pids=()
    
    for i in $(seq 1 $NUM_THREADS); do
        (
            for j in $(seq 1 $NUM_OPERATIONS); do
                echo "Thread $i, File $j" > "$MOUNT_POINT/thread_${i}_file_${j}.txt"
            done
        ) &
        pids+=($!)
    done
    
    for pid in "${pids[@]}"; do
        wait "$pid" || fail "Thread $pid failed during file creation"
    done
    
    local file_count=$(ls "$MOUNT_POINT" | wc -l) || fail "Failed to count files"
    local expected=$((NUM_THREADS * NUM_OPERATIONS))
    
    if [ "$file_count" -ne "$expected" ]; then
        fail "File count mismatch: expected $expected, got $file_count"
    fi
    
    for i in $(seq 1 $NUM_THREADS); do
        for j in $(seq 1 $NUM_OPERATIONS); do
            rm "$MOUNT_POINT/thread_${i}_file_${j}.txt" || fail "Failed to remove file"
        done
    done
    
    log "Concurrent file creation: PASSED ($file_count files created)"
}

test_concurrent_mixed() {
    log "Testing concurrent mixed read/write operations..."
    
    rm -f "$MOUNT_POINT/mixed_test.txt" || true
    echo "Initial content" > "$MOUNT_POINT/mixed_test.txt" || fail "Failed to create mixed_test.txt"
    
    local pids=()
    
    for i in $(seq 1 $NUM_THREADS); do
        (
            for j in $(seq 1 $((NUM_OPERATIONS / 2))); do
                echo "Write from thread $i" >> "$MOUNT_POINT/mixed_test.txt"
                local content=$(cat "$MOUNT_POINT/mixed_test.txt")
                if [ -z "$content" ]; then
                    echo "Empty content in thread $i" >&2
                    exit 1
                fi
            done
        ) &
        pids+=($!)
    done
    
    for pid in "${pids[@]}"; do
        wait "$pid" || fail "Thread $pid failed during mixed operations"
    done
    
    rm "$MOUNT_POINT/mixed_test.txt" || fail "Failed to remove mixed_test.txt"
    
    log "Concurrent mixed operations: PASSED"
}

trap cleanup EXIT

log "=== PowerFS Concurrent Test Suite ==="
log "Threads: $NUM_THREADS, Operations per thread: $NUM_OPERATIONS"

log "Creating mount point: $MOUNT_POINT"
mkdir -p "$MOUNT_POINT"

log "Loading powerfs module..."
if lsmod | grep -q "powerfs"; then
    sudo rmmod powerfs || fail "Failed to unload existing powerfs module"
fi
sudo insmod "$MODULE_PATH" || fail "Failed to load powerfs module"

log "Mounting powerfs at $MOUNT_POINT..."
sudo mount -t powerfs none "$MOUNT_POINT" || fail "Failed to mount powerfs"

test_concurrent_writes
test_concurrent_reads
test_concurrent_file_creation
test_concurrent_mixed

log "=== All concurrent tests PASSED ==="
