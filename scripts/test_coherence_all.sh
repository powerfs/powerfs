#!/bin/bash
# Comprehensive FUSE Coherence End-to-End Test Suite
# Runs all three phases of coherence tests

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

cd "$PROJECT_ROOT"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║    PowerFS FUSE Coherence E2E Test Suite                ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_SKIP=0

PHASE_RESULTS=()

run_phase() {
    local phase_num=$1
    local phase_name=$2
    local script=$3

    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Running Phase $phase_num: $phase_name"
    echo "═══════════════════════════════════════════════════════════"

    if bash "$SCRIPT_DIR/$script" 2>&1; then
        local result=0
    else
        local result=1
    fi

    PHASE_RESULTS+=("Phase $phase_num ($phase_name): exit code $result")
    return $result
}

# ============================================================
# Parse arguments
# ============================================================
RUN_PHASE0=true
RUN_PHASE1=true
RUN_PHASE2=true
RUN_PHASE3=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --phase0)
            RUN_PHASE0=true
            RUN_PHASE1=false
            RUN_PHASE2=false
            RUN_PHASE3=false
            shift
            ;;
        --phase1)
            RUN_PHASE0=false
            RUN_PHASE1=true
            RUN_PHASE2=false
            RUN_PHASE3=false
            shift
            ;;
        --phase2)
            RUN_PHASE0=false
            RUN_PHASE1=false
            RUN_PHASE2=true
            RUN_PHASE3=false
            shift
            ;;
        --phase3)
            RUN_PHASE0=false
            RUN_PHASE1=false
            RUN_PHASE2=false
            RUN_PHASE3=true
            shift
            ;;
        --phases)
            phases="$2"
            RUN_PHASE0=false
            RUN_PHASE1=false
            RUN_PHASE2=false
            RUN_PHASE3=false
            if echo "$phases" | grep -q "0"; then RUN_PHASE0=true; fi
            if echo "$phases" | grep -q "1"; then RUN_PHASE1=true; fi
            if echo "$phases" | grep -q "2"; then RUN_PHASE2=true; fi
            if echo "$phases" | grep -q "3"; then RUN_PHASE3=true; fi
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --phase0     Run only Phase 0 tests"
            echo "  --phase1     Run only Phase 1 tests"
            echo "  --phase2     Run only Phase 2 tests"
            echo "  --phase3     Run only Phase 3 tests"
            echo "  --phases N   Run specific phases (e.g., '0,1' or '023')"
            echo "  -h, --help   Show this help message"
            echo ""
            echo "Phases:"
            echo "  Phase 0: Synchronous commit + error rollback"
            echo "  Phase 1: Server-driven cache invalidation"
            echo "  Phase 2: Lease mechanism"
            echo "  Phase 3: Job-level strong consistency"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# ============================================================
# Pre-flight checks
# ============================================================
echo "Pre-flight checks..."

if ! command -v fusermount &> /dev/null && ! command -v umount &> /dev/null; then
    echo "[ERROR] No unmount command found (fusermount or umount required)"
    exit 1
fi

if ! command -v cargo &> /dev/null; then
    echo "[ERROR] cargo not found"
    exit 1
fi

echo "  ✓ Environment ready"
echo ""

# ============================================================
# Build first
# ============================================================
echo "Building binaries..."
cargo build -p powerfs-server -p powerfs-volume -p powerfs-fuse 2>&1 | tail -5
echo "  ✓ Build complete"
echo ""

# ============================================================
# Run phases
# ============================================================
OVERALL_RESULT=0

if [ "$RUN_PHASE0" = true ]; then
    if run_phase 0 "Synchronous Commit + Rollback" "test_coherence_phase0.sh"; then
        echo "  ✓ Phase 0 passed"
    else
        echo "  ✗ Phase 0 had failures"
        OVERALL_RESULT=1
    fi
fi

if [ "$RUN_PHASE1" = true ]; then
    if run_phase 1 "Server-Driven Cache Invalidation" "test_coherence_phase1.sh"; then
        echo "  ✓ Phase 1 passed"
    else
        echo "  ✗ Phase 1 had failures"
        OVERALL_RESULT=1
    fi
fi

if [ "$RUN_PHASE2" = true ]; then
    if run_phase 2 "Lease Mechanism" "test_coherence_phase2.sh"; then
        echo "  ✓ Phase 2 passed"
    else
        echo "  ✗ Phase 2 had failures"
        OVERALL_RESULT=1
    fi
fi

if [ "$RUN_PHASE3" = true ]; then
    if run_phase 3 "Job-Level Consistency" "test_coherence_phase3.sh"; then
        echo "  ✓ Phase 3 passed"
    else
        echo "  ✗ Phase 3 had failures"
        OVERALL_RESULT=1
    fi
fi

# ============================================================
# Final summary
# ============================================================
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
