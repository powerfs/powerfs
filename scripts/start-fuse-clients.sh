#!/bin/bash
# Start multiple FUSE client containers
#
# This script starts multiple PowerFS FUSE client containers in Docker,
# allowing concurrent client testing for coherence, failover, and consistency.
#
# Usage:
#   ./start-fuse-clients.sh                      # Start 2 FUSE clients (default)
#   ./start-fuse-clients.sh --count 5            # Start 5 FUSE clients
#   ./start-fuse-clients.sh --stop               # Stop all FUSE clients
#   ./start-fuse-clients.sh --clean              # Stop and remove FUSE containers
#   ./start-fuse-clients.sh --status             # Show running FUSE clients
#   ./start-fuse-clients.sh --exec 1 ls /mnt/powerfs  # Execute command on fuse-1
#   ./start-fuse-clients.sh --exec-all ls /mnt/powerfs  # Execute on all clients
#
# Environment variables:
#   FUSE_COUNT         - Number of FUSE clients to start (default: 2)
#   MASTER_ADDRESS     - Master gRPC address (default: 172.20.0.11:9333)
#   MOUNT_POINT        - FUSE mount point inside container (default: /mnt/powerfs)
#   COLLECTION         - Collection name (default: default)
#   REPLICATION        - Replication strategy (default: 000)
#   RUST_LOG_LEVEL     - Log level (default: debug)

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
DOCKER_DIR="$PROJECT_ROOT/docker"

FUSE_COUNT="${FUSE_COUNT:-2}"
MASTER_ADDRESS="${MASTER_ADDRESS:-172.20.0.11:9333}"
MOUNT_POINT="${MOUNT_POINT:-/mnt/powerfs}"
COLLECTION="${COLLECTION:-default}"
REPLICATION="${REPLICATION:-000}"
RUST_LOG_LEVEL="${RUST_LOG_LEVEL:-debug}"
NETWORK_NAME="docker_powerfs-network"

log_info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_warn() {
    echo "[WARN] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

print_banner() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║  PowerFS FUSE Client Cluster Starter                               ║"
    echo "║  Start multiple FUSE clients for coherence and failover testing    ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo ""
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        return 1
    fi
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        return 1
    fi
    return 0
}

check_network() {
    if ! docker network inspect "$NETWORK_NAME" &> /dev/null; then
        log_warn "Network $NETWORK_NAME does not exist"
        log_info "Creating network $NETWORK_NAME..."
        docker network create --driver bridge --subnet 172.20.0.0/16 "$NETWORK_NAME" &> /dev/null || true
    fi
    return 0
}

check_image() {
    if ! docker images | grep -q "^powerfs "; then
        log_info "Building powerfs image..."
        cd "$PROJECT_ROOT" && docker build -f docker/Dockerfile -t powerfs:latest .
    fi
    return 0
}

get_next_ip() {
    local start_ip=41
    local max_ip=254
    local used_ips=()
    
    for container in $(docker ps --filter "name=fuse-" --format "{{.Names}}"); do
        local ip=$(docker inspect "$container" | grep -o '"IPAddress": "[0-9.]\+"' | cut -d'"' -f4)
        if [ -n "$ip" ]; then
            used_ips+=("$ip")
        fi
    done
    
    for i in $(seq $start_ip $max_ip); do
        local candidate="172.20.0.$i"
        if ! [[ " ${used_ips[@]} " =~ " $candidate " ]]; then
            echo "$candidate"
            return 0
        fi
    done
    
    log_error "No available IPs in range 172.20.0.41-254"
    return 1
}

start_fuse_client() {
    local index=$1
    local container_name="fuse-$index"
    local ip=$(get_next_ip)
    
    if [ -z "$ip" ]; then
        log_error "Failed to get IP for fuse-$index"
        return 1
    fi
    
    if docker ps --filter "name=^/${container_name}$" --format "{{.Names}}" | grep -q "^${container_name}$"; then
        log_warn "Container $container_name is already running"
        return 0
    fi
    
    log_info "Starting FUSE client $container_name (IP: $ip)..."
    
    docker run -d \
        --name "$container_name" \
        --network "$NETWORK_NAME" \
        --ip "$ip" \
        --device /dev/fuse \
        --cap-add SYS_ADMIN \
        --cap-add DAC_READ_SEARCH \
        --security-opt apparmor:unconfined \
        --privileged \
        --restart unless-stopped \
        --label "powerfs=fuse-client" \
        powerfs:latest \
        bash -c "RUST_LOG=$RUST_LOG_LEVEL /app/powerfs-fuse \
            --master $MASTER_ADDRESS \
            --mount-point $MOUNT_POINT \
            --collection $COLLECTION \
            --replication $REPLICATION \
            --threads 8 \
            --verbose 2>&1 || sleep 30"
    
    if [ $? -eq 0 ]; then
        log_info "  Started: $container_name (IP: $ip)"
        return 0
    else
        log_error "  Failed to start: $container_name"
        return 1
    fi
}

stop_fuse_client() {
    local container_name=$1
    if docker ps --filter "name=^/${container_name}$" --format "{{.Names}}" | grep -q "^${container_name}$"; then
        log_info "Stopping $container_name..."
        docker stop "$container_name" &> /dev/null || true
    else
        log_warn "$container_name is not running"
    fi
}

remove_fuse_client() {
    local container_name=$1
    if docker ps -a --filter "name=^/${container_name}$" --format "{{.Names}}" | grep -q "^${container_name}$"; then
        log_info "Removing $container_name..."
        docker rm "$container_name" &> /dev/null || true
    fi
}

list_fuse_clients() {
    echo ""
    echo "┌────────────────────────────────────────────────────────────────────┐"
    echo "│  Running FUSE Clients                                             │"
    echo "├─────────────┬────────────────┬───────────────────────┬────────────┤"
    echo "│   Name      │     IP         │      Status           │  Mount     │"
    echo "├─────────────┼────────────────┼───────────────────────┼────────────┤"
    
    local count=0
    for container in $(docker ps --filter "label=powerfs=fuse-client" --format "{{.Names}}" | sort); do
        local ip=$(docker inspect "$container" | grep -o '"IPAddress": "[0-9.]\+"' | cut -d'"' -f4)
        local status=$(docker inspect "$container" | grep -o '"Status": "[^"]\+"' | cut -d'"' -f4)
        local mount=$(docker exec "$container" mount | grep "$MOUNT_POINT" | head -1 | cut -d' ' -f1 || echo "Not mounted")
        
        printf "│ %-11s │ %-14s │ %-21s │ %-10s │\n" \
            "$container" "$ip" "$status" "$mount"
        count=$((count + 1))
    done
    
    echo "└─────────────┴────────────────┴───────────────────────┴────────────┘"
    echo ""
    echo "Total FUSE clients: $count"
    echo ""
}

exec_on_client() {
    local index=$1
    shift
    local cmd="$*"
    local container_name="fuse-$index"
    
    if ! docker ps --filter "name=^/${container_name}$" --format "{{.Names}}" | grep -q "^${container_name}$"; then
        log_error "Container $container_name is not running"
        return 1
    fi
    
    echo "Executing on $container_name:"
    echo "  $cmd"
    echo ""
    docker exec "$container_name" bash -c "$cmd"
}

exec_on_all_clients() {
    local cmd="$*"
    
    for container in $(docker ps --filter "label=powerfs=fuse-client" --format "{{.Names}}" | sort); do
        echo ""
        echo "=== $container ==="
        docker exec "$container" bash -c "$cmd"
    done
}

main() {
    print_banner
    
    case "$1" in
        --stop)
            log_info "Stopping all FUSE clients..."
            for container in $(docker ps --filter "label=powerfs=fuse-client" --format "{{.Names}}"); do
                stop_fuse_client "$container"
            done
            log_info "All FUSE clients stopped"
            exit 0
            ;;
        --clean)
            log_info "Cleaning up all FUSE clients..."
            for container in $(docker ps -a --filter "label=powerfs=fuse-client" --format "{{.Names}}"); do
                stop_fuse_client "$container"
                remove_fuse_client "$container"
            done
            log_info "All FUSE clients cleaned up"
            exit 0
            ;;
        --status)
            list_fuse_clients
            exit 0
            ;;
        --exec)
            if [ -z "$2" ]; then
                log_error "Usage: --exec <index> <command>"
                exit 1
            fi
            exec_on_client "$2" "${@:3}"
            exit $?
            ;;
        --exec-all)
            if [ -z "$2" ]; then
                log_error "Usage: --exec-all <command>"
                exit 1
            fi
            exec_on_all_clients "${@:2}"
            exit $?
            ;;
        --count)
            if [ -n "$2" ]; then
                FUSE_COUNT="$2"
                shift 2
            else
                log_error "Usage: --count <number>"
                exit 1
            fi
            ;;
        *)
            ;;
    esac
    
    if ! check_docker; then
        exit 1
    fi
    
    check_network
    check_image
    
    log_info "Configuration:"
    log_info "  Clients:    $FUSE_COUNT"
    log_info "  Master:     $MASTER_ADDRESS"
    log_info "  Mount:      $MOUNT_POINT"
    log_info "  Collection: $COLLECTION"
    log_info "  Replication: $REPLICATION"
    log_info ""
    
    local failures=0
    for i in $(seq 1 $FUSE_COUNT); do
        if ! start_fuse_client "$i"; then
            failures=$((failures + 1))
        fi
    done
    
    echo ""
    if [ $failures -eq 0 ]; then
        log_info "=== Successfully started $FUSE_COUNT FUSE clients ==="
        list_fuse_clients
        echo "Quick commands:"
        echo "  Check status:    ./start-fuse-clients.sh --status"
        echo "  Exec on fuse-1:  ./start-fuse-clients.sh --exec 1 ls $MOUNT_POINT"
        echo "  Exec on all:     ./start-fuse-clients.sh --exec-all ls $MOUNT_POINT"
        echo "  Stop all:        ./start-fuse-clients.sh --stop"
        echo "  Clean up:        ./start-fuse-clients.sh --clean"
        exit 0
    else
        log_error "Failed to start $failures/$FUSE_COUNT FUSE clients"
        exit 1
    fi
}

main "$@"
