#!/bin/bash
# PowerFS Failover End-to-End Test
# Tests master outage recovery and failover mechanisms

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

source "$PROJECT_ROOT/scripts/lib/common.sh"

print_usage() {
    cat << EOF
PowerFS Failover E2E Test

Usage: $0 [OPTIONS]

Options:
  --clean         Clean up before running
  --check         Only check prerequisites
  --stop          Stop any running tests
  --help          Show this help message

EOF
}

check_prerequisites_only() {
    echo "=== Checking prerequisites ==="
    
    if ! command -v cargo &> /dev/null; then
        log_error "cargo is not installed"
        return 1
    fi
    log_success "cargo is installed"
    
    if [ ! -f "$PROJECT_ROOT/Cargo.toml" ]; then
        log_error "Cargo.toml not found"
        return 1
    fi
    log_success "Project root found: $PROJECT_ROOT"
    
    return 0
}

run_failover_tests() {
    cd "$PROJECT_ROOT"
    
    echo ""
    echo "============================================================"
    echo "  PowerFS Failover End-to-End Tests"
    echo "============================================================"
    echo ""
    
    echo "Running master outage tests..."
    echo ""
    
    # Run master outage tests
    if cargo test --package powerfs-master --test master_outage_e2e_test -- --test-threads=1 --nocapture 2>&1; then
        log_success "Master outage tests passed!"
    else
        log_error "Master outage tests failed!"
        return 1
    fi
    
    echo ""
    echo "Running failover coherence tests..."
    echo ""
    
    # Run failover coherence tests
    if cargo test --package powerfs-master --test coherence_failover_test -- --test-threads=1 --nocapture 2>&1; then
        log_success "Failover coherence tests passed!"
    else
        log_error "Failover coherence tests failed!"
        return 1
    fi
    
    echo ""
    log_success "All failover tests passed!"
    return 0
}

main() {
    local action="run"
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --clean)
                source "$PROJECT_ROOT/scripts/env/cleanup.sh"
                cleanup_test_env
                action="done"
                shift
                ;;
            --check)
                check_prerequisites_only
                exit $?
                ;;
            --stop)
                source "$PROJECT_ROOT/scripts/env/cleanup.sh"
                cleanup_test_env
                echo "Stopped"
                exit 0
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
    
    if [ "$action" = "run" ]; then
        check_prerequisites_only || exit 1
        run_failover_tests
    fi
}

main "$@"
