#!/bin/bash
# =============================================================================
# PowerFS Test Image Builder
#
# Builds:
#   powerfs:latest       — Base image with all PowerFS server binaries
#   powerfs-test:latest  — FUSE client + benchmark + protocol validation tools
#
# Usage:
#   ./docker/build_test_images.sh                # Build if images missing
#   ./docker/build_test_images.sh --force        # Force rebuild everything
#   ./docker/build_test_images.sh --skip-rust    # Skip cargo build
#   ./docker/build_test_images.sh --no-cache     # Docker build without cache
#
# Prerequisites:
#   - Rust toolchain (cargo, rustc)
#   - Docker + Docker Compose
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$SCRIPT_DIR"
LOG_DIR="/tmp/powerfs/logs"

FORCE=0
SKIP_RUST=0
NO_CACHE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)     FORCE=1 ;;
        --skip-rust) SKIP_RUST=1 ;;
        --no-cache)  NO_CACHE=1 ;;
        --help|-h)
            echo "Usage: $0 [--force] [--skip-rust] [--no-cache]"
            echo ""
            echo "  --force       Force full rebuild of Rust + Docker"
            echo "  --skip-rust   Skip cargo build (use existing target/release/)"
            echo "  --no-cache    Docker build without layer cache"
            exit 0
            ;;
        *) echo "Unknown option: $1 (use --help)"; exit 1 ;;
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

# ========== Setup PATH for cargo (sudo-safe) ==========
setup_cargo_path() {
    local cargo_paths=(
        "$HOME/.cargo/bin"
        "/home/portion/.cargo/bin"
        "/home/$SUDO_USER/.cargo/bin"
        "/root/.cargo/bin"
    )
    for p in "${cargo_paths[@]}"; do
        if [ -f "$p/cargo" ] && [ -f "$p/rustc" ]; then
            export PATH="$p:$PATH"
            log_info "Found Rust toolchain at: $p"
            return 0
        fi
    done
    return 1
}

# ========== Pre-flight ==========
check_tools() {
    log_step "Pre-flight Checks"

    if ! command -v cargo >/dev/null 2>&1; then
        setup_cargo_path
    fi

    if ! command -v cargo >/dev/null 2>&1 && [ "$SKIP_RUST" -eq 0 ]; then
        local binaries=("powerfs-master" "powerfs-filer" "powerfs-s3" "powerfs-volume" "powerfs-monitor" "powerfs-fuse" "powerfs-init" "powerfs-cli")
        local all_exist=1
        for bin in "${binaries[@]}"; do
            if [ ! -f "${PROJECT_ROOT}/target/release/${bin}" ]; then
                all_exist=0
                break
            fi
        done
        if [ "$all_exist" -eq 1 ]; then
            log_warn "Rust not found, but pre-built binaries exist. Auto-enabling --skip-rust."
            SKIP_RUST=1
        else
            log_error "Rust toolchain not found and no pre-built binaries!"
            exit 1
        fi
    fi
    log_pass "Cargo: $(cargo --version 2>/dev/null || echo 'skipped (pre-built binaries)')"

    if ! command -v docker >/dev/null 2>&1; then
        log_error "Docker not found."
        exit 1
    fi
    log_pass "Docker: $(docker --version)"

    if docker compose version >/dev/null 2>&1; then
        COMPOSE_CMD="docker compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        COMPOSE_CMD="docker-compose"
    else
        log_error "Docker Compose not found."
        exit 1
    fi
    log_pass "Compose: $(docker compose version 2>/dev/null || docker-compose --version)"

    if [ ! -f "$DOCKER_DIR/Dockerfile" ]; then
        log_error "Dockerfile not found: $DOCKER_DIR/Dockerfile"
        exit 1
    fi
    if [ ! -f "$DOCKER_DIR/Dockerfile-test" ]; then
        log_error "Dockerfile-test not found: $DOCKER_DIR/Dockerfile-test"
        exit 1
    fi
}

# ========== Rust Compilation ==========
build_rust() {
    if [ "$SKIP_RUST" -eq 1 ]; then
        log_info "Skipping Rust build (--skip-rust)"
        return 0
    fi

    local binaries=("powerfs-master" "powerfs-filer" "powerfs-s3" "powerfs-volume" "powerfs-monitor" "powerfs-fuse" "powerfs-init" "powerfs-cli")
    local all_exist=1
    for bin in "${binaries[@]}"; do
        if [ ! -f "${PROJECT_ROOT}/target/release/${bin}" ]; then
            all_exist=0
            break
        fi
    done
    if [ "$all_exist" -eq 1 ] && [ "$FORCE" -eq 0 ]; then
        log_info "All binaries already built (use --force to rebuild)"
        return 0
    fi

    log_step "Compiling Rust Binaries"
    log_info "  Project: $PROJECT_ROOT"
    cd "$PROJECT_ROOT"

    local start_time
    start_time=$(date +%s)

    cargo build --release 2>&1 | tee "$LOG_DIR/rust_build_$(date +%Y%m%d_%H%M%S).log"

    local end_time
    end_time=$(date +%s)
    log_pass "Cargo build completed in $((end_time - start_time))s"

    for bin in "${binaries[@]}"; do
        if [ -f "target/release/${bin}" ]; then
            local size
            size=$(stat -c '%s' "target/release/${bin}" 2>/dev/null)
            log_info "  $bin: $size bytes"
        else
            log_error "Binary not found: target/release/${bin}"
            exit 1
        fi
    done
}

# ========== Docker Build ==========
build_docker_images() {
    log_step "Building Docker Images"

    cd "$DOCKER_DIR"
    mkdir -p "$LOG_DIR"

    local docker_args=""
    [ "$NO_CACHE" -eq 1 ] && docker_args="--no-cache"

    if [ "$FORCE" -eq 1 ]; then
        log_info "Removing old images..."
        docker rmi powerfs:latest powerfs-test:latest 2>/dev/null || true
    fi

    # ---------- Build base image ----------
    log_info "Building powerfs:latest (base image)..."
    local build_start
    build_start=$(date +%s)

    docker build $docker_args \
        -t powerfs:latest \
        -f Dockerfile \
        "${PROJECT_ROOT}" 2>&1 | tee "$LOG_DIR/docker_build_$(date +%Y%m%d_%H%M%S).log"

    if [ "${PIPESTATUS[0]}" -ne 0 ]; then
        log_error "Docker base image build failed"
        exit 1
    fi
    log_pass "powerfs:latest built in $(($(date +%s) - build_start))s"

    # ---------- Build test image ----------
    log_info "Building powerfs-test:latest (FUSE client + benchmark)..."
    build_start=$(date +%s)

    docker build $docker_args \
        -t powerfs-test:latest \
        -f Dockerfile-test \
        "${PROJECT_ROOT}" 2>&1 | tee "$LOG_DIR/docker_build_test_$(date +%Y%m%d_%H%M%S).log"

    if [ "${PIPESTATUS[0]}" -ne 0 ]; then
        log_error "Docker test image build failed"
        exit 1
    fi
    log_pass "powerfs-test:latest built in $(($(date +%s) - build_start))s"

    # ---------- Verify ----------
    log_step "Image Verification"
    local base_size test_size
    base_size=$(docker image inspect powerfs:latest --format '{{.Size}}' 2>/dev/null || echo 'unknown')
    test_size=$(docker image inspect powerfs-test:latest --format '{{.Size}}' 2>/dev/null || echo 'unknown')
    log_pass "powerfs:latest       → ${base_size}"
    log_pass "powerfs-test:latest  → ${test_size}"

    log_info "Test image toolchain:"
    docker run --rm --entrypoint /bin/sh powerfs-test:latest -c \
        'echo "  fio: $(fio --version 2>/dev/null || echo missing)"; echo "  jq:  $(jq --version 2>/dev/null || echo missing)"; echo "  fuse3: $(fusermount3 -V 2>/dev/null || fusermount -V 2>/dev/null || echo missing)"; ls -1 /app/powerfs-* 2>/dev/null | head -10' 2>/dev/null || true
}

# ========== Summary ==========
show_summary() {
    echo ""
    echo -e "${G}╔══════════════════════════════════════════════════════════╗${N}"
    echo -e "${G}║  Build Complete — Test Images Ready                      ${N}"
    echo -e "${G}╚══════════════════════════════════════════════════════════╝${N}"
    echo ""
    echo "  Built images:"
    echo "    powerfs:latest       (base: master/volume/filer binaries)"
    echo "    powerfs-test:latest  (FUSE client + fio + protocol validation tools)"
    echo ""
    echo "  Next steps:"
    echo "    ./docker/start_test_env.sh          # Start full test cluster"
    echo "    ./docker/start_test_env.sh --wait   # Start + wait for FUSE ready"
    echo "    ./docker/stop_test_env.sh           # Stop test cluster"
    echo ""
}

# ========== Main ==========
main() {
    echo ""
    echo -e "${C}╔══════════════════════════════════════════════════════════╗${N}"
    echo -e "${C}║  PowerFS Test Image Builder                              ${N}"
    echo -e "${C}╚══════════════════════════════════════════════════════════╝${N}"
    echo ""
    echo -e "  ${B}Project:${N}      ${PROJECT_ROOT}"
    echo -e "  ${B}Docker dir:${N}   ${DOCKER_DIR}"
    echo -e "  ${B}Force:${N}        $([ "$FORCE" -eq 1 ] && echo 'yes' || echo 'no')"
    echo -e "  ${B}Skip Rust:${N}    $([ "$SKIP_RUST" -eq 1 ] && echo 'yes' || echo 'no')"
    echo -e "  ${B}No cache:${N}     $([ "$NO_CACHE" -eq 1 ] && echo 'yes' || echo 'no')"
    echo ""

    mkdir -p "$LOG_DIR"

    check_tools
    build_rust
    build_docker_images
    show_summary
}

main "$@"
