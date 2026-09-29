#!/bin/bash
# Build the self-contained PowerFS demo image from local release binaries.
# Usage: docker/demo/build.sh [image tag]   (default: powerfs-demo:dev)
set -euo pipefail

TAG="${1:-powerfs-demo:dev}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "==> building release binaries"
cargo build --release \
    -p powerfs-ctl -p powerfs-master -p powerfs-filer -p powerfs-volume -p powerfs-fuse

echo "==> building image $TAG"
docker build -f docker/demo/Dockerfile -t "$TAG" .

echo "==> done: $TAG"
