#!/usr/bin/env bash
# Thin wrapper: bring up the canonical PowerFS backend.
#
# P2 起主 compose 在仓库中不再手工维护，集群声明单一来源是
# <home>/cluster.toml —— 启动路径统一走 `powerfs-ctl bootstrap`
# （渲染 compose → 选主 → CA 签发 → 全服务 up → 健康门禁）。
#
# 用法：
#   ./docker/scripts/start-cluster.sh              # 使用默认 $REPO_ROOT/.powerfs
#   POWERFS_HOME=/path/to/home ./docker/scripts/start-cluster.sh
#
# EC 场景（docker/tests）需先 export 环境变量，bootstrap 时模板已暴露透传：
#   export POWERFS_EC_DATA_SHARDS=2
#   export POWERFS_EC_PARITY_SHARDS=1
#   export POWERFS_SCRUBBER_SCAN_INTERVAL=10
#   export POWERFS_SCRUBBER_MAX_INODES=50
#   export POWERFS_EC_MIN_FILE_SIZE=0
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOME_DIR="${POWERFS_HOME:-$REPO_ROOT/.powerfs}"
CTL="$REPO_ROOT/target/release/powerfs-ctl"

[ -x "$CTL" ] || { echo "error: $CTL missing — cargo build --release -p powerfs-ctl" >&2; exit 1; }
[ -f "$HOME_DIR/cluster.toml" ] || { echo "error: $HOME_DIR/cluster.toml missing — powerfs-ctl --home $HOME_DIR init" >&2; exit 1; }

# 可选：先 seed CA 从 docker/certs-default（兼容 CI runner / VM 客户端已签的证书）
CERTS_DEFAULT="$REPO_ROOT/docker/certs-default"
if [ -f "$CERTS_DEFAULT/ca.crt" ] && [ ! -f "$HOME_DIR/rendered/certs/ca.crt" ]; then
    echo "seeding master CA from docker/certs-default"
    mkdir -p "$HOME_DIR/rendered/certs"
    cp "$CERTS_DEFAULT/ca.crt" "$CERTS_DEFAULT/ca.key" \
       "$CERTS_DEFAULT/client_registry.json" "$HOME_DIR/rendered/certs/"
fi

echo "bootstrap: cluster.toml → rendered compose → quorum → up"
POWERFS_HOME="$HOME_DIR" "$CTL" bootstrap
echo "done"
