#!/usr/bin/env bash
# Regenerate the Python gRPC stubs bundled in the powerfs-kv package.
# Requires: pip install grpcio-tools
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$REPO_ROOT/powerfs-sdk/python/powerfs/proto"

mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
  -I "$REPO_ROOT/powerfs-master/proto" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  master.proto

# protoc emits a flat "import master_pb2"; rewrite to the in-package path.
sed -i 's/^import master_pb2 as master__pb2$/from powerfs.proto import master_pb2 as master__pb2/' \
  "$OUT_DIR/master_pb2_grpc.py"

touch "$OUT_DIR/__init__.py"
echo "stubs regenerated in $OUT_DIR"
