#!/bin/bash
# PowerFS Cleanup Script
# Cleans up all PowerFS processes, mounts, and data directories

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

source "$PROJECT_ROOT/scripts/lib/common.sh"

print_usage() {
    cat << EOF
PowerFS Environment Cleanup

Usage: $0 [OPTIONS]

Options:
  --force         Force kill all related processes
  --docker        Clean up Docker containers and volumes
  --all           Clean everything (default)
  --help          Show this help message

EOF
}

cleanup_mounts() {
    log_info "Cleaning up FUSE mounts..."
    
    # Common mount points
    local mount_points=(
        "/tmp/powerfs-test"
        "/tmp/powerfs-coherence-test"
        "/tmp/powerfs-perf-test"
        "/tmp/powerfs-posix-test"
        "/tmp/powerfs-open-test"
        "/tmp/powerfs-persistence-test"
        "/tmp/powerfs-fio-test"
        "/tmp/powerfs-bench-mount"
        "/tmp/powerfs/test"
    )
    
    for mp in "${mount_points[@]}"; do
        if mountpoint -q "$mp" 2>/dev/null; then
            log_info "Unmounting $mp..."
            fusermount -uz "$mp" 2>/dev/null || umount -f "$mp" 2>/dev/null || true
        fi
    done
    
    # Also check for any powerfs mounts
    mount | grep powerfs | awk '{print $3}' | while read -r mp; do
        log_info "Unmounting $mp..."
        fusermount -uz "$mp" 2>/dev/null || umount -f "$mp" 2>/dev/null || true
    done
    
    sleep 1
}

cleanup_processes() {
    log_info "Cleaning up PowerFS processes..."
    
    local processes=(
        "powerfs-master"
        "powerfs-filer"
        "powerfs-s3"
        "powerfs-volume"
        "powerfs-monitor"
        "powerfs-fuse"
    )
    
    for proc in "${processes[@]}"; do
        local pids
        pids=$(pgrep -f "$proc" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            log_info "Killing $proc processes: $pids"
            echo "$pids" | xargs kill -9 2>/dev/null || true
        fi
    done
    
    sleep 1
}

cleanup_data() {
    log_info "Cleaning up data directories..."
    
    local dirs=(
        "/tmp/powerfs-test"
        "/tmp/powerfs-coherence-test"
        "/tmp/powerfs-test-master"
        "/tmp/powerfs-test-volume"
        "/tmp/powerfs-test-filer"
        "/tmp/powerfs-coherence-master"
        "/tmp/powerfs-coherence-volume"
        "/tmp/powerfs-perf-test"
        "/tmp/powerfs-perf-data"
        "/tmp/powerfs-bench-mount"
        "/tmp/powerfs-posix-test"
        "/tmp/powerfs-posix-master"
        "/tmp/powerfs-posix-volume"
        "/tmp/powerfs-open-test"
        "/tmp/powerfs-open-master"
        "/tmp/powerfs-open-volume"
        "/tmp/powerfs-persistence-test"
        "/tmp/powerfs-persistence-master"
        "/tmp/powerfs-persistence-volume"
        "/tmp/powerfs-fio-test"
        "/tmp/powerfs-fio-master"
        "/tmp/powerfs-fio-volume"
    )
    
    for dir in "${dirs[@]}"; do
        if [ -d "$dir" ]; then
            rm -rf "$dir" 2>/dev/null || true
        fi
    done
    
    # Clean up any remaining powerfs tmp dirs
    find /tmp -maxdepth 1 -name "powerfs*" -type d 2>/dev/null | while read -r dir; do
        if [ -d "$dir" ]; then
            local mounted=false
            mount | grep -q "$dir" && mounted=true
            
            if [ "$mounted" = false ]; then
                rm -rf "$dir" 2>/dev/null || true
            fi
        fi
    done
}

cleanup_docker() {
    log_info "Cleaning up Docker environment..."
    
    if command -v docker &> /dev/null; then
        cd "$PROJECT_ROOT/docker"
        
        # Stop and remove containers
        docker compose -f docker-compose.test.yml down -v 2>/dev/null || true
        docker compose -f docker-compose.yml down -v 2>/dev/null || true
        docker compose -f docker-compose.crdt-test.yml down -v 2>/dev/null || true
        
        # Remove powerfs images
        docker rmi powerfs:latest 2>/dev/null || true
        docker rmi powerfs-test:latest 2>/dev/null || true
        
        # Prune dangling images
        docker image prune -f 2>/dev/null || true
    fi
}

main() {
    local force=false
    local do_docker=false
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --force)
                force=true
                shift
                ;;
            --docker)
                do_docker=true
                shift
                ;;
            --help|-h)
                print_usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done
    
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║     PowerFS Environment Cleanup                          ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    
    cleanup_mounts
    cleanup_processes
    cleanup_data
    
    if [ "$do_docker" = true ]; then
        cleanup_docker
    fi
    
    # Final check
    local remaining
    remaining=$(ps aux | grep powerfs | grep -v grep | grep -v docker || true)
    if [ -n "$remaining" ]; then
        log_warn "Some processes still running:"
        echo "$remaining"
        if [ "$force" = true ]; then
            log_info "Force killing remaining processes..."
            echo "$remaining" | awk '{print $2}' | xargs kill -9 2>/dev/null || true
        fi
    else
        log_success "All processes cleaned"
    fi
    
    echo ""
    log_success "Cleanup complete!"
}

main "$@"
