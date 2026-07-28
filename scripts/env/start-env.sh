#!/bin/bash
# Unified PowerFS Test Environment Start Script
# Supports both Docker-based and local binary testing

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

# Source common functions
source "$PROJECT_ROOT/scripts/lib/common.sh"

# Configuration
USE_DOCKER="${USE_DOCKER:-false}"
BUILD_FIRST="${BUILD_FIRST:-true}"
COMPOSE_FILE="${COMPOSE_FILE:-$PROJECT_ROOT/docker/docker-compose.test.yml}"

print_usage() {
    cat << EOF
PowerFS Test Environment Starter

Usage: $0 [OPTIONS]

Options:
  --docker        Use Docker Compose to start environment
  --local         Use local binaries (default)
  --no-build      Skip building binaries
  --build         Force build before starting
  --help          Show this help message

Examples:
  $0                    # Start local environment
  $0 --docker           # Start Docker environment
  $0 --local --no-build # Start local without building

EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --docker)
                USE_DOCKER=true
                shift
                ;;
            --local)
                USE_DOCKER=false
                shift
                ;;
            --no-build)
                BUILD_FIRST=false
                shift
                ;;
            --build)
                BUILD_FIRST=true
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
}

start_local_env() {
    log_info "Starting local test environment..."
    
    # Setup
    setup_test_env
    
    # Check prerequisites
    if ! check_prerequisites; then
        exit 1
    fi
    
    # Build if needed
    if [ "$BUILD_FIRST" = "true" ]; then
        build_binaries "release"
    fi
    
    # Cleanup any existing environment
    cleanup_test_env
    
    # Start all services
    start_all_services
    
    log_success "Test environment is ready!"
    echo ""
    echo "  Mount point: $MOUNT_DIR"
    echo "  Master port: $MASTER_PORT"
    echo "  Volume port: $VOLUME_PORT"
    echo "  Filer port:  $FILER_NET_PORT"
    echo ""
    echo "  To stop: $PROJECT_ROOT/scripts/env/stop-env.sh"
}

start_docker_env() {
    log_info "Starting Docker test environment..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    
    if ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not available"
        exit 1
    fi
    
    # Build image if needed
    if [ "$BUILD_FIRST" = "true" ]; then
        log_info "Building Docker image..."
        cd "$PROJECT_ROOT"
        cargo build --release -p powerfs-master -p powerfs-filer -p powerfs-s3 -p powerfs-volume -p powerfs-monitor -p powerfs-fuse 2>&1 | tail -3
        
        cd "$PROJECT_ROOT/docker"
        docker build -t powerfs:latest . 2>&1 | tail -5
    fi
    
    # Start environment
    log_info "Starting containers..."
    cd "$PROJECT_ROOT/docker"
    
    docker compose -f docker-compose.test.yml down 2>/dev/null || true
    docker compose -f docker-compose.test.yml up -d
    
    # Wait for services
    log_info "Waiting for services to be ready..."
    sleep 10
    
    # Check status
    docker compose -f docker-compose.test.yml ps
    
    log_success "Docker test environment is ready!"
}

main() {
    parse_args "$@"
    
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║     PowerFS Test Environment Starter                    ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    
    if [ "$USE_DOCKER" = "true" ]; then
        start_docker_env
    else
        start_local_env
    fi
}

main "$@"
