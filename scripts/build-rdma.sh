#!/bin/bash
#
# build-rdma.sh — PowerFS RDMA build wrapper (#47 硬化).
#
# Builds all server binaries with --features rdma so that RDMA transport
# (rdma_cm listener + AutoTransport) is compiled into the binary. Using this
# script instead of plain `cargo build --release` prevents the #47 incident:
# a TCP-only binary deployed into an RDMA environment silently starts without
# an RDMA listener, causing clients to fail RDMA connect and serve stale
# local cache → cross-client data divergence.
#
# The runtime guard (require_rdma=true in RDMA config files) is the second
# layer of defense: even if someone bypasses this script, the binary will
# refuse to start when require_rdma=true and the rdma feature is missing.
#
# Usage:
#   ./scripts/build-rdma.sh                # release build with rdma feature
#   ./scripts/build-rdma.sh --debug        # debug build
#   ./scripts/build-rdma.sh --check        # cargo check only (no binary output)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Fresh, unique build id per invocation (see build.sh for rationale).
if [ -z "${POWERFS_BUILD_ID:-}" ]; then
  export POWERFS_BUILD_ID="$(date +%s%N)-$$-$(hostname 2>/dev/null || echo host)"
fi

cd "${ROOT_DIR}"

MODE="--release"
ACTION="build"
for arg in "$@"; do
  case "$arg" in
    --debug)  MODE="" ;;
    --check) ACTION="check" ;;
    *) echo "Unknown option: $arg" >&2; exit 1 ;;
  esac
done

BINS=(
  -p powerfs-master
  -p powerfs-filer
  -p powerfs-volume
  -p powerfs-monitor
  -p powerfs-cli
  -p powerfs-init
)

echo "==> POWERFS_BUILD_ID=${POWERFS_BUILD_ID}"
echo "==> cargo ${ACTION} ${MODE} --features rdma ${BINS[*]}"
exec cargo "${ACTION}" ${MODE} --features rdma "${BINS[@]}"
