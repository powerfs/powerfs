#!/bin/bash
#
# build.sh — PowerFS build wrapper.
#
# Exports a fresh POWERFS_BUILD_ID before invoking cargo so that
# powerfs-common/build.rs re-runs on every invocation, stamping an updated
# build timestamp and a unique build id into the binary. This makes two
# binaries produced from the same commit distinguishable and ensures the
# startup "Build Info" log always reflects the actual build time.
#
# Usage:
#   ./scripts/build.sh                # cargo build (release-equivalent default args)
#   ./scripts/build.sh --release      # forward extra args to cargo
#   ./scripts/build.sh build --features enterprise
#   POWERFS_BUILD_ID=custom ./scripts/build.sh   # override the id
#
# Any leading "build" subcommand is optional; remaining args are forwarded
# to cargo verbatim. To run a different cargo subcommand (e.g. test, clippy),
# pass it explicitly: ./scripts/build.sh check, ./scripts/build.sh test.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Fresh, unique build id per invocation: unix nanoseconds + pid + hostname.
# Exported so powerfs-common/build.rs (which declares
# `cargo:rerun-if-env-changed=POWERFS_BUILD_ID`) re-runs and produces a new
# BUILD_TIME / POWERFS_BUILD_ID pair even on incremental builds.
if [ -z "${POWERFS_BUILD_ID:-}" ]; then
  export POWERFS_BUILD_ID="$(date +%s%N)-$$-$(hostname 2>/dev/null || echo host)"
fi

cd "${ROOT_DIR}"

# Allow callers to omit the leading "build" subcommand for convenience.
if [ "$#" -gt 0 ] && [ "$1" != "build" ] && [ "$1" != "check" ] && [ "$1" != "test" ] && [ "$1" != "clippy" ] && [ "$1" != "run" ]; then
  # First arg is not a known cargo subcommand → assume build flags.
  set -- build "$@"
fi

echo "==> POWERFS_BUILD_ID=${POWERFS_BUILD_ID}"
echo "==> cargo $*"
exec cargo "$@"