#!/bin/bash

set -e

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1"
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $1"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SUCCESS] $1"
}

wait_for_master() {
    local master_name=$1
    local timeout=${2:-30}
    
    log_info "Waiting for $master_name to be healthy..."
    local count=0
    while [ $count -lt $timeout ]; do
        if docker inspect --format='{{.State.Health.Status}}' "$master_name" 2>/dev/null | grep -q healthy; then
            log_success "$master_name is healthy"
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done
    log_error "$master_name failed to become healthy within $timeout seconds"
    return 1
}

wait_for_volume_reconnect() {
    local volume_name=$1
    local target_master=$2
    local timeout=${3:-60}
    
    log_info "Waiting for $volume_name to connect to $target_master..."
    local count=0
    while [ $count -lt $timeout ]; do
        if docker logs "$volume_name" 2>&1 | grep -q "Connected to master: $target_master"; then
            log_success "$volume_name connected to $target_master"
            return 0
        fi
        sleep 2
        count=$((count + 2))
    done
    log_error "$volume_name failed to connect to $target_master within $timeout seconds"
    return 1
}

test_fuse_write() {
    local test_file=$1
    local content=$2
    
    log_info "Testing FUSE write: $test_file"
    if docker exec fuse-1 bash -c "echo '$content' > /mnt/powerfs/$test_file && cat /mnt/powerfs/$test_file" | grep -q "$content"; then
        log_success "FUSE write successful"
        return 0
    else
        log_error "FUSE write failed"
        return 1
    fi
}

print_master_status() {
    log_info "=== Master Status ==="
    docker ps --filter "name=master" --format "table {{.Names}}\t{{.Status}}" 2>/dev/null || echo "No master containers found"
    echo ""
}

print_volume_status() {
    log_info "=== Volume Connection Status ==="
    for v in volume-1 volume-2 volume-3; do
        local last_conn=$(docker logs "$v" 2>&1 | grep "Connected to master:" | tail -1 | awk '{print $NF}')
        echo "  $v: $last_conn"
    done
    echo ""
}

cleanup() {
    log_info "Cleaning up test files..."
    docker exec fuse-1 bash -c "rm -f /mnt/powerfs/failover_test_*.txt" 2>/dev/null || true
}

run_test() {
    cleanup
    
    log_info "============================================"
    log_info " PowerFS Master Failover Test Suite"
    log_info "============================================"
    
    local test_pass=0
    local test_fail=0
    
    log_info ""
    log_info "--- Test 1: Initial state - All masters up ---"
    print_master_status
    print_volume_status
    
    if test_fuse_write "failover_test_1.txt" "initial_state"; then
        test_pass=$((test_pass + 1))
    else
        test_fail=$((test_fail + 1))
    fi
    
    log_info ""
    log_info "--- Test 2: Stop master-1 and master-2, keep only master-3 ---"
    log_info "Stopping master-1..."
    docker stop master-1 2>/dev/null || true
    log_info "Stopping master-2..."
    docker stop master-2 2>/dev/null || true
    
    sleep 5
    print_master_status
    
    log_info "Waiting for volume nodes to reconnect to master-3..."
    if wait_for_volume_reconnect "volume-1" "172.20.0.13:9333" 90; then
        test_pass=$((test_pass + 1))
    else
        test_fail=$((test_fail + 1))
    fi
    
    print_volume_status
    
    if test_fuse_write "failover_test_2.txt" "master3_only"; then
        test_pass=$((test_pass + 1))
    else
        test_fail=$((test_fail + 1))
    fi
    
    log_info ""
    log_info "--- Test 3: Restart master-1 and master-2 ---"
    log_info "Starting master-1..."
    docker start master-1 2>/dev/null || true
    log_info "Starting master-2..."
    docker start master-2 2>/dev/null || true
    
    wait_for_master "master-1" 60
    wait_for_master "master-2" 60
    
    sleep 5
    print_master_status
    print_volume_status
    
    if test_fuse_write "failover_test_3.txt" "all_masters_back"; then
        test_pass=$((test_pass + 1))
    else
        test_fail=$((test_fail + 1))
    fi
    
    log_info ""
    log_info "--- Test 4: Stop master-3, trigger leader change to master-1 ---"
    log_info "Stopping master-3..."
    docker stop master-3 2>/dev/null || true
    
    sleep 5
    print_master_status
    
    log_info "Waiting for volume nodes to reconnect to master-1 or master-2..."
    local v1_connected=false
    local count=0
    while [ $count -lt 90 ]; do
        if docker logs volume-1 2>&1 | grep -q "Connected to master: 172.20.0.11:9333"; then
            v1_connected=true
            break
        elif docker logs volume-1 2>&1 | grep -q "Connected to master: 172.20.0.12:9333"; then
            v1_connected=true
            break
        fi
        sleep 2
        count=$((count + 2))
    done
    
    if $v1_connected; then
        log_success "Volume-1 reconnected to master-1 or master-2"
        test_pass=$((test_pass + 1))
    else
        log_error "Volume-1 failed to reconnect"
        test_fail=$((test_fail + 1))
    fi
    
    print_volume_status
    
    if test_fuse_write "failover_test_4.txt" "leader_changed"; then
        test_pass=$((test_pass + 1))
    else
        test_fail=$((test_fail + 1))
    fi
    
    log_info ""
    log_info "--- Test 5: Restart master-3 and verify full recovery ---"
    log_info "Starting master-3..."
    docker start master-3 2>/dev/null || true
    
    wait_for_master "master-3" 60
    
    sleep 5
    print_master_status
    print_volume_status
    
    if test_fuse_write "failover_test_5.txt" "full_recovery"; then
        test_pass=$((test_pass + 1))
    else
        test_fail=$((test_fail + 1))
    fi
    
    log_info ""
    log_info "============================================"
    log_info " Test Results Summary"
    log_info "============================================"
    log_info "Passed: $test_pass"
    log_info "Failed: $test_fail"
    
    if [ $test_fail -eq 0 ]; then
        log_success "All tests passed!"
        cleanup
        exit 0
    else
        log_error "$test_fail tests failed!"
        cleanup
        exit 1
    fi
}

cd "$DOCKER_DIR"

if [ ! -f "docker-compose.yml" ]; then
    log_error "docker-compose.yml not found in $DOCKER_DIR"
    exit 1
fi

run_test
