#!/bin/bash
# =============================================================================
# PowerFS Test Environment Startup Script
#
# Starts the full test cluster using docker-compose.test.yml:
#   Redis → Masters → Volumes → Init Filers → Filers → FUSE Client
#
# Usage:
#   ./docker/start_test_env.sh                # Start cluster
#   ./docker/start_test_env.sh --wait         # Start + wait for FUSE mount ready
#   ./docker/start_test_env.sh --build        # Rebuild images before starting
#   ./docker/start_test_env.sh --backend-only # Start backend only (no FUSE)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR"
PROJECT_ROOT="$(cd "$DOCKER_DIR/.." && pwd)"
COMPOSE_FILE="$DOCKER_DIR/docker-compose.test.yml"

WAIT=0
BUILD=0
BACKEND_ONLY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --wait)         WAIT=1 ;;
        --build)        BUILD=1 ;;
        --backend-only) BACKEND_ONLY=1 ;;
        --help|-h)
            echo "Usage: $0 [--wait] [--build] [--backend-only]"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# Color output
if [ -t 1 ]; then
    R='\033[0;31m'; G='\033[0;32m'; Y='\033[0;33m'
    B='\033[0;34m'; C='\033[0;36m'; N='\033[0m'
else
    R=''; G=''; Y=''; B=''; C=''; N=''
fi
log_info()  { echo -e "${B}[INFO]${N}  $(date +%H:%M:%S) $*"; }
log_pass()  { echo -e "${G}[PASS]${N}  $*"; }
log_warn()  { echo -e "${Y}[WARN]${N} $*"; }
log_error() { echo -e "${R}[ERROR]${N} $*"; }
log_step()  { echo -e "\n${C}━━━ $* ━━━${N}"; }

COMPOSE_CMD="docker compose"
if ! $COMPOSE_CMD version >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
fi

# ========== Build images if requested ==========
if [ "$BUILD" -eq 1 ]; then
    log_step "Building Docker Images"
    "$DOCKER_DIR/build_test_images.sh" --skip-rust
fi

# ========== Pre-flight: check images exist ==========
check_images() {
    log_step "Pre-flight: Image Check"
    local missing=0
    for img in powerfs:latest powerfs-test:latest; do
        if ! docker image inspect "$img" >/dev/null 2>&1; then
            log_error "Image not found: $img"
            missing=1
        else
            log_pass "Image exists: $img"
        fi
    done
    if [ "$missing" -eq 1 ]; then
        log_error "Required images missing. Run: ./docker/build_test_images.sh"
        exit 1
    fi
}

# ========== Cleanup existing ==========
cleanup_existing() {
    log_step "Cleaning Up Existing Containers"
    cd "$DOCKER_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
    log_pass "Old containers removed"
}

# ========== Wait for healthy ==========
wait_healthy() {
    local name="$1"
    local timeout="${2:-60}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local status
        status=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null || echo "missing")
        if [ "$status" = "healthy" ]; then
            log_pass "$name healthy (${elapsed}s)"
            return 0
        elif [ "$status" = "unhealthy" ]; then
            log_warn "$name unhealthy after ${elapsed}s"
            return 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    log_warn "$name not healthy after ${timeout}s (status: $status)"
    return 1
}

# ========== Start sequence ==========
start_redis() {
    log_step "[1/6] Starting Redis"
    cd "$DOCKER_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d redis
    wait_healthy "redis-test" 30 || log_warn "Redis not healthy, continuing..."
}

start_masters() {
    log_step "[2/6] Starting Master Nodes"
    cd "$DOCKER_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d master-1
    wait_healthy "master-1-test" 60 || log_warn "master-1 not healthy"

    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d master-2
    wait_healthy "master-2-test" 60 || log_warn "master-2 not healthy"

    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d master-3
    wait_healthy "master-3-test" 60 || log_warn "master-3 not healthy"
}

start_volumes() {
    log_step "[3/6] Starting Volume Nodes"
    cd "$DOCKER_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d volume-1 volume-2 volume-3
    for name in volume-1-test volume-2-test volume-3-test; do
        wait_healthy "$name" 30 || log_warn "$name not healthy"
    done
}

start_init_filers() {
    log_step "[4/6] Initializing Filer Metadata"
    cd "$DOCKER_DIR"
    # init-filer containers run once and exit
    $COMPOSE_CMD -f "$COMPOSE_FILE" up --abort-on-container-exit init-filer-1 init-filer-2 init-filer-3 2>&1 | tail -10 || true
    log_pass "Filer metadata initialized"
}

start_filers() {
    log_step "[5/6] Starting Filer Nodes"
    cd "$DOCKER_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d filer-1 filer-2 filer-3
    for name in filer-1-test filer-2-test filer-3-test; do
        wait_healthy "$name" 60 || log_warn "$name not healthy"
    done
}

start_fuse() {
    log_step "[6/6] Starting FUSE Client"
    cd "$DOCKER_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d fuse-test

    if [ "$WAIT" -eq 1 ]; then
        log_info "Waiting for FUSE mount to be ready..."
        local timeout=60
        local elapsed=0
        while [ "$elapsed" -lt "$timeout" ]; do
            if docker exec fuse-test sh -c 'mount | grep -q /mnt/fuse' 2>/dev/null; then
                log_pass "FUSE mounted at /mnt/fuse (${elapsed}s)"
                return 0
            fi
            sleep 2
            elapsed=$((elapsed + 2))
        done
        log_error "FUSE mount failed after ${timeout}s"
        log_info "Checking fuse-test logs:"
        docker logs fuse-test 2>&1 | tail -30
        return 1
    else
        log_pass "FUSE container started (use --wait to wait for mount)"
    fi
}

# ========== Summary ==========
show_summary() {
    echo ""
    echo -e "${G}╔══════════════════════════════════════════════════════════╗${N}"
    echo -e "${G}║  Test Cluster Started                                    ${N}"
    echo -e "${G}╚══════════════════════════════════════════════════════════╝${N}"
    echo ""
    echo "  Container Status:"
    docker ps --filter "name=-test" --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || true
    echo ""
    echo "  Service Endpoints (host):"
    echo "    Master 1:  localhost:9433 (gRPC), localhost:9434 (net)"
    echo "    Volume 1:  localhost:8180 (gRPC), localhost:8191 (http)"
    echo "    Filer 1:   localhost:8988 (http), localhost:8989 (gRPC), localhost:8990 (net)"
    echo "    Redis:     localhost:6380"
    echo ""
    if [ "$BACKEND_ONLY" -eq 0 ]; then
        echo "  FUSE Mount:"
        echo "    Container: fuse-test"
        echo "    Mount point: /mnt/fuse (inside container)"
        echo "    Test inside container: docker exec -it fuse-test ls /mnt/fuse"
        echo ""
        echo "  Run P1 protocol validation:"
        echo "    ./scripts/test_p1_fuse.sh"
        echo ""
    fi
    echo "  Stop cluster:"
    echo "    ./docker/stop_test_env.sh"
    echo ""
}

# ========== Main ==========
main() {
    echo ""
    echo -e "${C}╔══════════════════════════════════════════════════════════╗${N}"
    echo -e "${C}║  PowerFS Test Environment Startup                        ${N}"
    echo -e "${C}╚══════════════════════════════════════════════════════════╝${N}"
    echo ""
    echo -e "  ${B}Compose file:${N}  ${COMPOSE_FILE}"
    echo -e "  ${B}Wait for FUSE:${N} $([ "$WAIT" -eq 1 ] && echo 'yes' || echo 'no')"
    echo -e "  ${B}Backend only:${N}  $([ "$BACKEND_ONLY" -eq 1 ] && echo 'yes' || echo 'no')"
    echo ""

    check_images
    cleanup_existing
    start_redis
    start_masters
    start_volumes
    start_init_filers
    start_filers

    if [ "$BACKEND_ONLY" -eq 0 ]; then
        start_fuse
    else
        log_info "Skipping FUSE (--backend-only)"
    fi

    show_summary
}

main "$@"
