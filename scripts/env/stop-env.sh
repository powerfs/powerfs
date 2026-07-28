#!/bin/bash
# Unified PowerFS Test Environment Stop Script

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

# Source common functions
source "$PROJECT_ROOT/scripts/lib/common.sh"

print_usage() {
    cat << EOF
PowerFS Test Environment Stopper

Usage: $0 [OPTIONS]

Options:
  --docker        Stop Docker environment
  --local         Stop local environment (default)
  --all           Stop both Docker and local
  --help          Show this help message

EOF
}

stop_local_env() {
    log_info "Stopping local test environment..."
    
    setup_test_env
    cleanup_test_env
    
    log_success "Local environment stopped"
}

stop_docker_env() {
    log_info "Stopping Docker test environment..."
    
    cd "$PROJECT_ROOT/docker"
    docker compose -f docker-compose.test.yml down 2>/dev/null || true
    
    log_success "Docker environment stopped"
}

stop_all() {
    stop_local_env
    stop_docker_env
}

main() {
    local mode="local"
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --docker)
                mode="docker"
                shift
                ;;
            --local)
                mode="local"
                shift
                ;;
            --all)
                mode="all"
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
    echo "║     PowerFS Test Environment Stopper                    ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    
    case "$mode" in
        docker)
            stop_docker_env
            ;;
        local)
            stop_local_env
            ;;
        all)
            stop_all
            ;;
    esac
}

main "$@"
