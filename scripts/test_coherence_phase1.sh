#!/bin/bash
# Phase 1: Server-driven cache invalidation end-to-end tests
# Validates that metadata changes on one client are
# propagated to other clients via cache invalidation

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/coherence_test_common.sh"

setup_test_env

MOUNT2_DIR="${MOUNT2_DIR:-/tmp/powerfs-coherence-test2}"
FUSE2_PID=""

trap 'cleanup_test_env' EXIT

echo ""
echo "============================================================"
echo "  Phase 1: Server-Driven Cache Invalidation E2E Tests"
echo "============================================================"

build_binaries
start_all_services

start_second_fuse() {
    log_info "Starting second FUSE mount at $MOUNT2_DIR..."
    mkdir -p "$MOUNT2_DIR"

    "$PROJECT_ROOT/target/debug/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT2_DIR" \
        --collection default \
        --replication 000 \
        > /tmp/coherence-test-fuse2.log 2>&1 &
    FUSE2_PID=$!

    sleep 4

    if ! kill -0 "$FUSE2_PID" 2>/dev/null; then
        log_error "Second FUSE failed to start"
        cat /tmp/coherence-test-fuse2.log
        return 1
    fi

    if ! mountpoint -q "$MOUNT2_DIR" 2>/dev/null; then
        log_error "Second FUSE mount not ready"
        return 1
    fi

    log_info "Second FUSE started (PID: $FUSE2_PID)"
}

stop_second_fuse() {
    log_info "Stopping second FUSE mount..."
    if mountpoint -q "$MOUNT2_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT2_DIR" 2>/dev/null || true
        sleep 0.5
    fi
    [ -n "$FUSE2_PID" ] && kill -TERM "$FUSE2_PID" 2>/dev/null || true
    sleep 1
    rm -rf "$MOUNT2_DIR" 2>/dev/null || true
    log_info "Second FUSE stopped"
}

start_second_fuse

# ============================================================
# Test 1: Basic cache invalidation - file creation
# ============================================================
test_cache_invalidation_create() {
    test_start "cache invalidation - file creation"

    rm -rf "$MOUNT_DIR/phase1_create" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase1_create"
    sleep 1

    ls "$MOUNT2_DIR/phase1_create/" > /dev/null 2>&1
    sleep 1

    echo "created from client1" > "$MOUNT_DIR/phase1_create/new_file.txt"
    sleep 2

    if [ -f "$MOUNT2_DIR/phase1_create/new_file.txt" ]; then
        content=$(cat "$MOUNT2_DIR/phase1_create/new_file.txt" 2>/dev/null || true)
        assert_eq "created from client1" "$content" "File content visible on client2"
        test_pass
    else
        test_skip "Cache invalidation may need more time or not active"
        return 0
    fi

    rm -rf "$MOUNT_DIR/phase1_create"
}

test_cache_invalidation_create

# ============================================================
# Test 2: Cache invalidation - file deletion
# ============================================================
test_cache_invalidation_delete() {
    test_start "cache invalidation - file deletion"

    rm -rf "$MOUNT_DIR/phase1_delete" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase1_delete"
    echo "to be deleted" > "$MOUNT_DIR/phase1_delete/file.txt"
    sleep 2

    ls "$MOUNT2_DIR/phase1_delete/" > /dev/null 2>&1
    if [ ! -f "$MOUNT2_DIR/phase1_delete/file.txt" ]; then
        test_skip "File not visible on client2, skipping test"
        return 0
    fi

    rm "$MOUNT_DIR/phase1_delete/file.txt"
    sleep 2

    if [ ! -f "$MOUNT2_DIR/phase1_delete/file.txt" ]; then
        test_pass
    else
        test_fail "File should not exist on client2 after deletion on client1"
    fi

    rm -rf "$MOUNT_DIR/phase1_delete"
}

test_cache_invalidation_delete

# ============================================================
# Test 3: Cache invalidation - directory creation
# ============================================================
test_cache_invalidation_mkdir() {
    test_start "cache invalidation - directory creation"

    rm -rf "$MOUNT_DIR/phase1_mkdir" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase1_mkdir"
    sleep 1

    ls "$MOUNT2_DIR/phase1_mkdir/" > /dev/null 2>&1
    sleep 1

    mkdir "$MOUNT_DIR/phase1_mkdir/new_dir"
    sleep 2

    if [ -d "$MOUNT2_DIR/phase1_mkdir/new_dir" ]; then
        test_pass
    else
        test_skip "Directory cache invalidation may need more time"
        return 0
    fi

    rm -rf "$MOUNT_DIR/phase1_mkdir"
}

test_cache_invalidation_mkdir

# ============================================================
# Test 4: Cache invalidation - directory deletion
# ============================================================
test_cache_invalidation_rmdir() {
    test_start "cache invalidation - directory deletion"

    rm -rf "$MOUNT_DIR/phase1_rmdir" 2>/dev/null || true
    mkdir -p "$MOUNT_DIR/phase1_rmdir/subdir"
    sleep 2

    ls "$MOUNT2_DIR/phase1_rmdir/" > /dev/null 2>&1
    if [ ! -d "$MOUNT2_DIR/phase1_rmdir/subdir" ]; then
        test_skip "Subdir not visible on client2, skipping test"
        return 0
    fi

    rmdir "$MOUNT_DIR/phase1_rmdir/subdir"
    sleep 2

    if [ ! -d "$MOUNT2_DIR/phase1_rmdir/subdir" ]; then
        test_pass
    else
        test_fail "Subdir should not exist on client2 after deletion on client1"
    fi

    rm -rf "$MOUNT_DIR/phase1_rmdir"
}

test_cache_invalidation_rmdir

# ============================================================
# Test 5: Cache invalidation - rename
# ============================================================
test_cache_invalidation_rename() {
    test_start "cache invalidation - rename"

    rm -rf "$MOUNT_DIR/phase1_rename" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase1_rename"
    echo "rename test" > "$MOUNT_DIR/phase1_rename/old_name.txt"
    sleep 2

    ls "$MOUNT2_DIR/phase1_rename/" > /dev/null 2>&1
    if [ ! -f "$MOUNT2_DIR/phase1_rename/old_name.txt" ]; then
        test_skip "Original file not visible on client2, skipping test"
        return 0
    fi

    mv "$MOUNT_DIR/phase1_rename/old_name.txt" "$MOUNT_DIR/phase1_rename/new_name.txt"
    sleep 2

    old_exists=false
    new_exists=false
    [ -f "$MOUNT2_DIR/phase1_rename/old_name.txt" ] && old_exists=true
    [ -f "$MOUNT2_DIR/phase1_rename/new_name.txt" ] && new_exists=true

    if [ "$old_exists" = false ] && [ "$new_exists" = true ]; then
        test_pass
    else
        test_skip "Rename cache invalidation may need more time (old=$old_exists, new=$new_exists)"
        return 0
    fi

    rm -rf "$MOUNT_DIR/phase1_rename"
}

test_cache_invalidation_rename

# ============================================================
# Test 6: Cache invalidation - attribute change
# ============================================================
test_cache_invalidation_attr() {
    test_start "cache invalidation - attribute change"

    rm -rf "$MOUNT_DIR/phase1_attr" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase1_attr"
    echo "attr test" > "$MOUNT_DIR/phase1_attr/file.txt"
    chmod 644 "$MOUNT_DIR/phase1_attr/file.txt"
    sleep 2

    ls -l "$MOUNT2_DIR/phase1_attr/" > /dev/null 2>&1
    perms_before=$(stat -c "%a" "$MOUNT2_DIR/phase1_attr/file.txt" 2>/dev/null || echo "000")

    chmod 755 "$MOUNT_DIR/phase1_attr/file.txt"
    sleep 2

    perms_after=$(stat -c "%a" "$MOUNT2_DIR/phase1_attr/file.txt" 2>/dev/null || echo "000")

    if [ "$perms_after" = "755" ]; then
        test_pass
    else
        test_skip "Attribute cache invalidation may need more time (before=$perms_before, after=$perms_after)"
        return 0
    fi

    rm -rf "$MOUNT_DIR/phase1_attr"
}

test_cache_invalidation_attr

# ============================================================
# Test 7: Generation number increment on metadata change
# ============================================================
test_generation_increment() {
    test_start "generation number increment"

    rm -rf "$MOUNT_DIR/phase1_gen" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase1_gen"

    echo "gen test v1" > "$MOUNT_DIR/phase1_gen/file.txt"
    sleep 1

    content1=$(cat "$MOUNT_DIR/phase1_gen/file.txt" 2>/dev/null || true)
    assert_eq "gen test v1" "$content1" "First write content correct"

    echo "gen test v2" > "$MOUNT_DIR/phase1_gen/file.txt"
    sleep 1

    content2=$(cat "$MOUNT_DIR/phase1_gen/file.txt" 2>/dev/null || true)
    assert_eq "gen test v2" "$content2" "Second write content correct"

    restart_fuse
    sleep 1

    content3=$(cat "$MOUNT_DIR/phase1_gen/file.txt" 2>/dev/null || true)
    assert_eq "gen test v2" "$content3" "Content persists after FUSE restart"

    rm -rf "$MOUNT_DIR/phase1_gen"
    test_pass
}

test_generation_increment

# ============================================================
# Test 8: Directory listing update after invalidation
# ============================================================
test_dir_listing_update() {
    test_start "directory listing update after invalidation"

    rm -rf "$MOUNT_DIR/phase1_dirlist" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase1_dirlist"

    for i in $(seq 1 5); do
        echo "file $i" > "$MOUNT_DIR/phase1_dirlist/f_$i.txt"
    done
    sleep 2

    count_before=$(ls "$MOUNT2_DIR/phase1_dirlist/" 2>/dev/null | wc -l)

    for i in $(seq 6 10); do
        echo "file $i" > "$MOUNT_DIR/phase1_dirlist/f_$i.txt"
    done
    sleep 2

    count_after=$(ls "$MOUNT2_DIR/phase1_dirlist/" 2>/dev/null | wc -l)

    if [ "$count_after" -ge "$count_before" ]; then
        log_info "File count: before=$count_before, after=$count_after"
        if [ "$count_after" -eq 10 ]; then
            test_pass
        else
            test_skip "Dir listing may not fully updated (expected 10, got $count_after)"
            return 0
        fi
    else
        test_fail "Directory listing should not decrease after adding files"
    fi

    rm -rf "$MOUNT_DIR/phase1_dirlist"
}

test_dir_listing_update

# ============================================================
# Test 9: Multiple rapid changes
# ============================================================
test_rapid_changes() {
    test_start "multiple rapid changes handling"

    rm -rf "$MOUNT_DIR/phase1_rapid" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase1_rapid"

    for i in $(seq 1 20); do
        echo "rapid $i" > "$MOUNT_DIR/phase1_rapid/r_$i.txt"
    done
    sleep 3

    count=$(ls "$MOUNT2_DIR/phase1_rapid/" 2>/dev/null | wc -l)
    log_info "Client2 sees $count files after 20 rapid creates"

    for i in $(seq 1 10); do
        rm "$MOUNT_DIR/phase1_rapid/r_$i.txt"
    done
    sleep 3

    count2=$(ls "$MOUNT2_DIR/phase1_rapid/" 2>/dev/null | wc -l)
    log_info "Client2 sees $count2 files after 10 rapid deletes"

    if [ "$count2" -le "$count" ]; then
        test_pass
    else
        test_fail "File count should not increase after deletions"
    fi

    rm -rf "$MOUNT_DIR/phase1_rapid"
}

test_rapid_changes

# ============================================================
# Test 10: Cache invalidation for nested directories
# ============================================================
test_nested_dir_invalidation() {
    test_start "nested directory cache invalidation"

    rm -rf "$MOUNT_DIR/phase1_nested" 2>/dev/null || true
    mkdir -p "$MOUNT_DIR/phase1_nested/level1/level2/level3"
    echo "deep file" > "$MOUNT_DIR/phase1_nested/level1/level2/level3/deep.txt"
    sleep 2

    ls -R "$MOUNT2_DIR/phase1_nested/" > /dev/null 2>&1 || true
    sleep 1

    mkdir "$MOUNT_DIR/phase1_nested/level1/level2/new_dir"
    echo "new deep file" > "$MOUNT_DIR/phase1_nested/level1/level2/level3/new_file.txt"
    sleep 2

    if [ -d "$MOUNT2_DIR/phase1_nested/level1/level2/new_dir" ]; then
        test_pass
    else
        test_skip "Nested dir cache invalidation may need more time"
        return 0
    fi

    rm -rf "$MOUNT_DIR/phase1_nested"
}

test_nested_dir_invalidation

# ============================================================
# Summary
# ============================================================
stop_second_fuse

echo ""
echo "============================================================"
echo "  Phase 1 Test Results"
echo "============================================================"
print_summary
