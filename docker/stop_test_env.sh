#!/bin/bash
# =============================================================================
# PowerFS Test Environment Stop Script
#
# Stops the test cluster started by docker/start_test_env.sh.
# Follows the user's preference: use start/stop scripts instead of pkill/rm.
#
# Usage:
#   ./docker/stop_test_env.sh             # Stop containers, keep volumes
#   ./docker/stop_test_env.sh --volumes   # Stop + remove volumes (data loss)
#   ./docker/stop_test_env.sh --clean     # Stop + remove volumes + images
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR"
COMPOSE_FILE="$DOCKER_DIR/docker-compose.test.yml"

REMOVE_VOLUMES=0
REMOVE_IMAGES=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --volumes|-v) REMOVE_VOLUMES=1 ;;
        --clean|-c)   REMOVE_VOLUMES=1; REMOVE_IMAGES=1 ;;
        --help|-h)
            echo "Usage: $0 [--volumes] [--clean]"
            echo ""
            echo "  --volumes, -v  Remove persistent volumes (data loss!)"
            echo "  --clean, -c    Remove volumes + images (full reset)"
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

echo ""
echo -e "${C}╔══════════════════════════════════════════════════════════╗${N}"
echo -e "${C}║  PowerFS Test Environment Shutdown                       ${N}"
echo -e "${C}╚══════════════════════════════════════════════════════════╝${N}"
echo ""

# ========== Step 1: Graceful stop ==========
log_step "[1/3] Stopping Containers"
cd "$DOCKER_DIR"

# Graceful down (sends SIGTERM, waits, then SIGKILL)
$COMPOSE_CMD -f "$COMPOSE_FILE" down --remove-orphans 2>&1 | tail -5
log_pass "Containers stopped"

# ========== Step 2: Remove volumes if requested ==========
if [ "$REMOVE_VOLUMES" -eq 1 ]; then
    log_step "[2/3] Removing Persistent Volumes"
    $COMPOSE_CMD -f "$COMPOSE_FILE" down -v --remove-orphans 2>&1 | tail -3
    log_pass "Volumes removed"
else
    log_step "[2/3] Preserving Persistent Volumes"
    log_info "Use --volumes to remove data volumes"
fi

# ========== Step 3: Remove images if requested ==========
if [ "$REMOVE_IMAGES" -eq 1 ]; then
    log_step "[3/3] Removing Docker Images"
    docker rmi powerfs:latest powerfs-test:latest 2>/dev/null || true
    log_pass "Images removed"
else
    log_step "[3/3] Preserving Docker Images"
    log_info "Use --clean to remove images"
fi

# ========== Verify ==========
log_step "Verification"
remaining=$(docker ps --filter "name=-test" --format "{{.Names}}" 2>/dev/null | wc -l)
if [ "$remaining" -eq 0 ]; then
    log_pass "All test containers removed"
else
    log_warn "$remaining test containers still running:"
    docker ps --filter "name=-test" --format "  {{.Names}}\t{{.Status}}" 2>/dev/null
fi

echo ""
echo -e "${G}╔══════════════════════════════════════════════════════════╗${N}"
echo -e "${G}║  Test Cluster Stopped                                    ${N}"
echo -e "${G}╚══════════════════════════════════════════════════════════╝${N}"
echo ""
echo "  To restart:"
echo "    ./docker/start_test_env.sh --wait"
echo ""
