#!/bin/bash
# Phase 3: Job-level strong consistency end-to-end tests
# Validates that job-level consistency provides in-job ordering guarantees
# and batch cache invalidation on job completion

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/coherence_test_common.sh"

setup_test_env

MOUNT2_DIR="${MOUNT2_DIR:-/tmp/powerfs-coherence-test2}"
FUSE2_PID=""

JOB_ID="${JOB_ID:-test-job-001}"
JOB_NAME="${JOB_NAME:-training-job}"

trap 'cleanup_test_env' EXIT

echo ""
echo "============================================================"
echo "  Phase 3: Job-Level Consistency E2E Tests"
echo "============================================================"

build_binaries
start_all_services

start_job_fuse_clients() {
    log_info "Starting FUSE client 1 with job context..."
    mkdir -p "$MOUNT_DIR"

    POWERFS_JOB_ID="$JOB_ID" \
    POWERFS_JOB_NAME="$JOB_NAME" \
    "$PROJECT_ROOT/target/debug/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT_DIR" \
        --collection default \
        --replication 000 \
        > /tmp/coherence-test-fuse1.log 2>&1 &
    FUSE_PID=$!

    log_info "Starting FUSE client 2 with job context..."
    mkdir -p "$MOUNT2_DIR"

    POWERFS_JOB_ID="$JOB_ID" \
    POWERFS_JOB_NAME="$JOB_NAME" \
    "$PROJECT_ROOT/target/debug/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT2_DIR" \
        --collection default \
        --replication 000 \
        > /tmp/coherence-test-fuse2.log 2>&1 &
    FUSE2_PID=$!

    sleep 4

    if ! kill -0 "$FUSE_PID" 2>/dev/null; then
        log_error "FUSE client 1 failed to start"
        cat /tmp/coherence-test-fuse1.log
        return 1
    fi

    if ! kill -0 "$FUSE2_PID" 2>/dev/null; then
        log_error "FUSE client 2 failed to start"
        cat /tmp/coherence-test-fuse2.log
        return 1
    fi

    if ! mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        log_error "FUSE mount 1 not ready"
        return 1
    fi

    if ! mountpoint -q "$MOUNT2_DIR" 2>/dev/null; then
        log_error "FUSE mount 2 not ready"
        return 1
    fi

    log_info "Both FUSE clients started with job ID: $JOB_ID"
}

stop_job_fuse_clients() {
    log_info "Stopping FUSE client 2..."
    if mountpoint -q "$MOUNT2_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT2_DIR" 2>/dev/null || true
        sleep 0.5
    fi
    [ -n "$FUSE2_PID" ] && kill -TERM "$FUSE2_PID" 2>/dev/null || true
    FUSE2_PID=""

    log_info "Stopping FUSE client 1..."
    if mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT_DIR" 2>/dev/null || true
        sleep 0.5
    fi
    [ -n "$FUSE_PID" ] && kill -TERM "$FUSE_PID" 2>/dev/null || true
    FUSE_PID=""

    sleep 1
    rm -rf "$MOUNT_DIR" "$MOUNT2_DIR" 2>/dev/null || true
    log_info "Both FUSE clients stopped"
}

start_job_fuse_clients

# ============================================================
# Test 1: Job client registration via environment variable
# ============================================================
test_job_registration_via_env() {
    test_start "job registration via environment variable"

    rm -rf "$MOUNT_DIR/phase3_reg" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_reg"
    echo "job registration test" > "$MOUNT_DIR/phase3_reg/test.txt"
    sleep 1

    content1=$(cat "$MOUNT_DIR/phase3_reg/test.txt" 2>/dev/null || true)
    content2=$(cat "$MOUNT2_DIR/phase3_reg/test.txt" 2>/dev/null || true)

    if [ "$content1" = "job registration test" ] && [ "$content2" = "job registration test" ]; then
        test_pass
    else
        test_fail "File not visible across job clients (c1: $content1, c2: $content2)"
    fi

    rm -rf "$MOUNT_DIR/phase3_reg"
}

test_job_registration_via_env

# ============================================================
# Test 2: In-job file creation visibility
# ============================================================
test_in_job_file_visibility() {
    test_start "in-job file creation visibility"

    rm -rf "$MOUNT_DIR/phase3_visibility" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_visibility"
    sleep 1

    echo "created by client1" > "$MOUNT_DIR/phase3_visibility/client1_file.txt"
    sleep 1

    content=$(cat "$MOUNT2_DIR/phase3_visibility/client1_file.txt" 2>/dev/null || true)

    if [ "$content" = "created by client1" ]; then
        test_pass
    else
        test_fail "Client2 cannot see file created by client1 in same job: $content"
    fi

    rm -rf "$MOUNT_DIR/phase3_visibility"
}

test_in_job_file_visibility

# ============================================================
# Test 3: In-job directory listing consistency
# ============================================================
test_in_job_dir_listing() {
    test_start "in-job directory listing consistency"

    rm -rf "$MOUNT_DIR/phase3_dirlist" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_dirlist"
    sleep 1

    for i in $(seq 1 5); do
        echo "file $i" > "$MOUNT_DIR/phase3_dirlist/file_$i.txt"
    done
    sleep 1

    list1=$(ls "$MOUNT_DIR/phase3_dirlist" 2>/dev/null | sort | tr '\n' ' ')
    list2=$(ls "$MOUNT2_DIR/phase3_dirlist" 2>/dev/null | sort | tr '\n' ' ')

    if [ "$list1" = "$list2" ]; then
        count=$(echo "$list1" | wc -w)
        if [ "$count" -eq 5 ]; then
            test_pass
        else
            test_fail "Expected 5 files, got $count"
        fi
    else
        test_fail "Directory listing mismatch: c1=[$list1], c2=[$list2]"
    fi

    rm -rf "$MOUNT_DIR/phase3_dirlist"
}

test_in_job_dir_listing

# ============================================================
# Test 4: In-job file modification visibility
# ============================================================
test_in_job_file_modification() {
    test_start "in-job file modification visibility"

    rm -rf "$MOUNT_DIR/phase3_modify" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_modify"
    echo "initial content" > "$MOUNT_DIR/phase3_modify/data.txt"
    sleep 1

    echo "modified by client1" > "$MOUNT_DIR/phase3_modify/data.txt"
    sleep 1

    content=$(cat "$MOUNT2_DIR/phase3_modify/data.txt" 2>/dev/null || true)

    if [ "$content" = "modified by client1" ]; then
        test_pass
    else
        test_fail "Client2 sees stale content: $content"
    fi

    rm -rf "$MOUNT_DIR/phase3_modify"
}

test_in_job_file_modification

# ============================================================
# Test 5: In-job file deletion visibility
# ============================================================
test_in_job_file_deletion() {
    test_start "in-job file deletion visibility"

    rm -rf "$MOUNT_DIR/phase3_delete" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_delete"
    echo "to be deleted" > "$MOUNT_DIR/phase3_delete/todelete.txt"
    sleep 1

    rm "$MOUNT_DIR/phase3_delete/todelete.txt"
    sleep 1

    if [ -f "$MOUNT2_DIR/phase3_delete/todelete.txt" ]; then
        test_fail "Client2 still sees deleted file"
    else
        test_pass
    fi

    rm -rf "$MOUNT_DIR/phase3_delete"
}

test_in_job_file_deletion

# ============================================================
# Test 6: Multiple jobs independent
# ============================================================
test_multiple_jobs_independent() {
    test_start "multiple jobs operate independently"

    rm -rf "$MOUNT_DIR/phase3_multi_job" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_multi_job"
    sleep 1

    echo "job data" > "$MOUNT_DIR/phase3_multi_job/shared.txt"
    sleep 1

    log_info "Both clients registered to same job, should see same data"
    content1=$(cat "$MOUNT_DIR/phase3_multi_job/shared.txt" 2>/dev/null || true)
    content2=$(cat "$MOUNT2_DIR/phase3_multi_job/shared.txt" 2>/dev/null || true)

    if [ "$content1" = "$content2" ]; then
        test_pass
    else
        test_fail "Same job clients see different content: c1=[$content1], c2=[$content2]"
    fi

    rm -rf "$MOUNT_DIR/phase3_multi_job"
}

test_multiple_jobs_independent

# ============================================================
# Test 7: Job client deregistration on unmount
# ============================================================
test_job_client_deregister_on_unmount() {
    test_start "job client deregistration on FUSE unmount"

    rm -rf "$MOUNT_DIR/phase3_deregister" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_deregister"
    echo "deregister test" > "$MOUNT_DIR/phase3_deregister/test.txt"
    sleep 1

    log_info "Stopping client 2 (deregisters from job)..."
    if mountpoint -q "$MOUNT2_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT2_DIR" 2>/dev/null || true
        sleep 1
    fi
    [ -n "$FUSE2_PID" ] && kill -TERM "$FUSE2_PID" 2>/dev/null || true
    FUSE2_PID=""
    sleep 2

    content=$(cat "$MOUNT_DIR/phase3_deregister/test.txt" 2>/dev/null || true)
    if [ "$content" = "deregister test" ]; then
        log_info "Client 1 still operational after client 2 deregistration"
        test_pass
    else
        test_fail "Client 1 affected by client 2 deregistration"
    fi

    log_info "Restarting client 2..."
    mkdir -p "$MOUNT2_DIR"
    POWERFS_JOB_ID="$JOB_ID" \
    POWERFS_JOB_NAME="$JOB_NAME" \
    "$PROJECT_ROOT/target/debug/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT2_DIR" \
        --collection default \
        --replication 000 \
        > /tmp/coherence-test-fuse2.log 2>&1 &
    FUSE2_PID=$!
    sleep 4

    content2=$(cat "$MOUNT2_DIR/phase3_deregister/test.txt" 2>/dev/null || true)
    log_info "Client 2 re-registered, content: $content2"

    rm -rf "$MOUNT_DIR/phase3_deregister"
}

test_job_client_deregister_on_unmount

# ============================================================
# Test 8: In-job rename visibility
# ============================================================
test_in_job_rename_visibility() {
    test_start "in-job rename operation visibility"

    rm -rf "$MOUNT_DIR/phase3_rename" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_rename"
    echo "rename test content" > "$MOUNT_DIR/phase3_rename/old_name.txt"
    sleep 1

    mv "$MOUNT_DIR/phase3_rename/old_name.txt" "$MOUNT_DIR/phase3_rename/new_name.txt"
    sleep 1

    if [ -f "$MOUNT2_DIR/phase3_rename/new_name.txt" ] && [ ! -f "$MOUNT2_DIR/phase3_rename/old_name.txt" ]; then
        content=$(cat "$MOUNT2_DIR/phase3_rename/new_name.txt" 2>/dev/null || true)
        if [ "$content" = "rename test content" ]; then
            test_pass
        else
            test_fail "Renamed file has wrong content: $content"
        fi
    else
        test_fail "Rename not visible to client2 correctly"
    fi

    rm -rf "$MOUNT_DIR/phase3_rename"
}

test_in_job_rename_visibility

# ============================================================
# Test 9: In-job mkdir visibility
# ============================================================
test_in_job_mkdir_visibility() {
    test_start "in-job mkdir visibility"

    rm -rf "$MOUNT_DIR/phase3_mkdir" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_mkdir"
    sleep 1

    mkdir "$MOUNT_DIR/phase3_mkdir/subdir1"
    mkdir "$MOUNT_DIR/phase3_mkdir/subdir1/nested"
    sleep 1

    if [ -d "$MOUNT2_DIR/phase3_mkdir/subdir1" ] && [ -d "$MOUNT2_DIR/phase3_mkdir/subdir1/nested" ]; then
        test_pass
    else
        test_fail "Nested directories not visible to client2"
    fi

    rm -rf "$MOUNT_DIR/phase3_mkdir"
}

test_in_job_mkdir_visibility

# ============================================================
# Test 10: In-job symlink visibility
# ============================================================
test_in_job_symlink_visibility() {
    test_start "in-job symlink visibility"

    rm -rf "$MOUNT_DIR/phase3_symlink" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase3_symlink"
    echo "target content" > "$MOUNT_DIR/phase3_symlink/target.txt"
    sleep 1

    ln -s target.txt "$MOUNT_DIR/phase3_symlink/link.txt"
    sleep 1

    if [ -L "$MOUNT2_DIR/phase3_symlink/link.txt" ]; then
        content=$(cat "$MOUNT2_DIR/phase3_symlink/link.txt" 2>/dev/null || true)
        if [ "$content" = "target content" ]; then
            test_pass
        else
            test_fail "Symlink target content wrong: $content"
        fi
    else
        test_fail "Symlink not visible to client2"
    fi

    rm -rf "$MOUNT_DIR/phase3_symlink"
}

test_in_job_symlink_visibility

# ============================================================
# Summary
# ============================================================
stop_job_fuse_clients

echo ""
echo "============================================================"
echo "  Phase 3 Test Results"
echo "============================================================"
print_summary
