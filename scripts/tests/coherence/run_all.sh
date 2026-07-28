#!/bin/bash
# Unified Coherence Test Runner
# Runs all phases of coherence tests

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

print_usage() {
    cat << EOF
PowerFS Coherence Test Suite

Usage: $0 [OPTIONS]

Options:
  --phase0     Run only Phase 0 tests (synchronous commit)
  --phase1     Run only Phase 1 tests (cache invalidation)
  --phase2     Run only Phase 2 tests (lease mechanism)
  --phase3     Run only Phase 3 tests (job consistency)
  --phases N   Run specific phases (e.g., '0,1,2')
  --help       Show this help message

Phases:
  Phase 0: Synchronous commit + error rollback
  Phase 1: Server-driven cache invalidation
  Phase 2: Lease mechanism (integration tests)
  Phase 3: Job-level strong consistency (Rust integration tests)

EOF
}

# Parse arguments
RUN_PHASE0=true
RUN_PHASE1=true
RUN_PHASE2=true
RUN_PHASE3=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --phase0) RUN_PHASE0=true; RUN_PHASE1=false; RUN_PHASE2=false; RUN_PHASE3=false; shift ;;
        --phase1) RUN_PHASE0=false; RUN_PHASE1=true; RUN_PHASE2=false; RUN_PHASE3=false; shift ;;
        --phase2) RUN_PHASE0=false; RUN_PHASE1=false; RUN_PHASE2=true; RUN_PHASE3=false; shift ;;
        --phase3) RUN_PHASE0=false; RUN_PHASE1=false; RUN_PHASE2=false; RUN_PHASE3=true; shift ;;
        --phases)
            phases="$2"
            RUN_PHASE0=false; RUN_PHASE1=false; RUN_PHASE2=false; RUN_PHASE3=false
            if echo "$phases" | grep -q "0"; then RUN_PHASE0=true; fi
            if echo "$phases" | grep -q "1"; then RUN_PHASE1=true; fi
            if echo "$phases" | grep -q "2"; then RUN_PHASE2=true; fi
            if echo "$phases" | grep -q "3"; then RUN_PHASE3=true; fi
            shift 2
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     PowerFS Coherence Test Suite                        ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

PHASE_RESULTS=()
OVERALL_RESULT=0

run_phase_script() {
    local phase_num=$1
    local phase_name=$2
    local script=$3
    
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Running Phase $phase_num: $phase_name"
    echo "═══════════════════════════════════════════════════════════"
    
    if bash "$SCRIPT_DIR/$script" 2>&1; then
        echo -e "  \033[0;32m✓ Phase $phase_num passed\033[0m"
        PHASE_RESULTS+=("Phase $phase_num ($phase_name): PASS")
    else
        echo -e "  \033[0;31m✗ Phase $phase_num had failures\033[0m"
        PHASE_RESULTS+=("Phase $phase_num ($phase_name): FAIL")
        OVERALL_RESULT=1
    fi
}

# Run selected phases
if [ "$RUN_PHASE0" = "true" ]; then
    run_phase_script 0 "Synchronous Commit + Rollback" "phase0_sync.sh"
fi

if [ "$RUN_PHASE1" = "true" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Running Phase 1: Server-Driven Cache Invalidation"
    echo "═══════════════════════════════════════════════════════════"
    echo "  Note: Phase 1 tests (cache invalidation)"
    echo "  will be added when multi-client support is ready"
    echo ""
    echo -e "  \033[0;33m⚠ Phase 1: Not yet available\033[0m"
    PHASE_RESULTS+=("Phase 1 (Cache Invalidation): SKIP - Not available")
fi

if [ "$RUN_PHASE2" = "true" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Running Phase 2: Lease Mechanism"
    echo "═══════════════════════════════════════════════════════════"
    echo "  Note: Phase 2 tests are Rust integration tests"
    echo ""
    
    cd "$PROJECT_ROOT"
    
    if cargo test --package powerfs-master --test coherence_phase2_test --verbose 2>&1 | tail -20; then
        echo -e "  \033[0;32m✓ Phase 2 passed\033[0m"
        PHASE_RESULTS+=("Phase 2 (Lease Mechanism): PASS")
    else
        echo -e "  \033[0;31m✗ Phase 2 had failures\033[0m"
        PHASE_RESULTS+=("Phase 2 (Lease Mechanism): FAIL")
        OVERALL_RESULT=1
    fi
fi

if [ "$RUN_PHASE3" = "true" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Running Phase 3: Job-Level Consistency"
    echo "═══════════════════════════════════════════════════════════"
    echo "  Note: Phase 3 tests are Rust integration tests"
    echo ""
    
    cd "$PROJECT_ROOT"
    
    if cargo test --package powerfs-master --test coherence_phase3_test --verbose 2>&1 | tail -20; then
        echo -e "  \033[0;32m✓ Phase 3 passed\033[0m"
        PHASE_RESULTS+=("Phase 3 (Job Consistency): PASS")
    else
        echo -e "  \033[0;31m✗ Phase 3 had failures\033[0m"
        PHASE_RESULTS+=("Phase 3 (Job Consistency): FAIL")
        OVERALL_RESULT=1
    fi
fi

# Final summary
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║    FINAL SUMMARY                                         ║"
echo "╠══════════════════════════════════════════════════════════╣"
for result in "${PHASE_RESULTS[@]}"; do
    echo "║  $result"
done
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

if [ "$OVERALL_RESULT" -eq 0 ]; then
    echo "🎉 All selected phases completed successfully!"
else
    echo "⚠️  Some tests had failures. Check above for details."
fi

exit $OVERALL_RESULT
