#!/bin/bash
#
# build-deb.sh — Build all PowerFS .deb packages via cargo-deb.
#
# Produces one .deb per role under target/debian/:
#   powerfs-cli_0.1.0_amd64.deb           (base, required by all others)
#   powerfs-master_0.1.0_amd64.deb
#   powerfs-volume_0.1.0_amd64.deb
#   powerfs-filer_0.1.0_amd64.deb
#   powerfs-fuse_0.1.0_amd64.deb
#   powerfs-s3_0.1.0_amd64.deb
#   powerfs-monitor_0.1.0_amd64.deb
#   powerfs-init_0.1.0_amd64.deb
#
# Usage:
#   ./scripts/build-deb.sh                 # build all packages
#   ./scripts/build-deb.sh powerfs-master  # build only one package
#   ./scripts/build-deb.sh --install-cargo-deb  # install cargo-deb first
#
# Prerequisites:
#   - Rust toolchain (stable)
#   - cargo-deb (auto-installed if missing, unless --no-install is set)
#   - dpkg-dev / dpkg-deb (usually preinstalled on Debian/Ubuntu)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# All packages that should be built, in dependency order (cli first).
ALL_PACKAGES=(
    powerfs-cli
    powerfs-master
    powerfs-volume
    powerfs-filer
    powerfs-fuse
    powerfs-s3
    powerfs-monitor
    powerfs-init
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log()  { echo -e "${BLUE}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}[$(date +%H:%M:%S)] OK${NC}    $*"; }
warn() { echo -e "${YELLOW}[$(date +%H:%M:%S)] WARN${NC}  $*"; }
err()  { echo -e "${RED}[$(date +%H:%M:%S)] ERR${NC}   $*" >&2; }

# Parse args
INSTALL_CARGO_DEB=1
PACKAGES=()
for arg in "$@"; do
    case "$arg" in
        --install-cargo-deb) INSTALL_CARGO_DEB=1 ;;
        --no-install)        INSTALL_CARGO_DEB=0 ;;
        --help|-h)
            sed -n '2,20p' "$0"
            exit 0
            ;;
        *)
            PACKAGES+=("$arg")
            ;;
    esac
done

# Default: build all packages
if [ "${#PACKAGES[@]}" -eq 0 ]; then
    PACKAGES=("${ALL_PACKAGES[@]}")
fi

cd "${ROOT_DIR}"

# Ensure cargo is available
if ! command -v cargo >/dev/null 2>&1; then
    if [ -f "$HOME/.cargo/env" ]; then
        # shellcheck disable=SC1091
        source "$HOME/.cargo/env"
    else
        err "cargo not found. Install Rust toolchain: https://rustup.rs"
        exit 1
    fi
fi

# Ensure cargo-deb is available
if ! cargo deb --version >/dev/null 2>&1; then
    if [ "${INSTALL_CARGO_DEB}" -eq 1 ]; then
        log "Installing cargo-deb..."
        cargo install cargo-deb
    else
        err "cargo-deb not installed. Run: cargo install cargo-deb"
        exit 1
    fi
fi

# Verify packaging assets exist
for f in \
    powerfs-packaging/scripts/postinst \
    powerfs-packaging/scripts/prerm \
    powerfs-packaging/scripts/postrm \
    powerfs-packaging/config-templates/powerfs.toml.template \
    powerfs-packaging/config-templates/powerfs.env; do
    if [ ! -f "${ROOT_DIR}/${f}" ]; then
        err "Missing packaging asset: ${f}"
        exit 1
    fi
done

for svc in master volume filer fuse s3 monitor; do
    if [ ! -f "${ROOT_DIR}/powerfs-packaging/systemd/powerfs-${svc}.service" ]; then
        err "Missing systemd unit: powerfs-packaging/systemd/powerfs-${svc}.service"
        exit 1
    fi
done
ok "Packaging assets verified"

# Step 1: Build all release binaries first (faster than letting cargo-deb rebuild per package)
log "Building release binaries for packages: ${PACKAGES[*]}"
BUILD_PKGS=()
for pkg in "${PACKAGES[@]}"; do
    BUILD_PKGS+=("-p" "${pkg}")
done
cargo build --release "${BUILD_PKGS[@]}"
ok "Release binaries built"

# Step 2: Build each .deb package
log "Building .deb packages..."
FAILED=()
BUILT=()
for pkg in "${PACKAGES[@]}"; do
    log "  -> cargo deb --package ${pkg}"
    if cargo deb --package "${pkg}" --no-build 2>&1; then
        BUILT+=("${pkg}")
    else
        FAILED+=("${pkg}")
        err "Failed to build ${pkg}"
    fi
done

# Step 3: Collect & report
DEB_DIR="${ROOT_DIR}/target/debian"
log ""
log "==================== Build Report ===================="
if [ "${#BUILT[@]}" -gt 0 ]; then
    ok "Built ${#BUILT[@]} package(s):"
    for pkg in "${BUILT[@]}"; do
        # Find the actual .deb file
        deb_file=$(ls "${DEB_DIR}"/${pkg}_*.deb 2>/dev/null | head -1 || true)
        if [ -n "${deb_file}" ]; then
            size=$(du -h "${deb_file}" | cut -f1)
            echo "    ${deb_file}  (${size})"
        else
            warn "    ${pkg}: .deb file not found in ${DEB_DIR}"
        fi
    done
fi

if [ "${#FAILED[@]}" -gt 0 ]; then
    err "Failed ${#FAILED[@]} package(s): ${FAILED[*]}"
    exit 1
fi

log ""
log "Next steps:"
log "  1. Copy .deb files to target nodes"
log "  2. On each node: sudo dpkg -i powerfs-cli_*.deb <role-package>.deb"
log "  3. Generate configs:  powerfs-cli config gen --masters ... --output /etc/powerfs"
log "  4. (Filer only) init: powerfs-init --config /etc/powerfs/filer-1.toml"
log "  5. Start service:     sudo systemctl start powerfs-<role>"
log ""
ok "Done."
