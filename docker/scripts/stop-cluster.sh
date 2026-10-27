#!/usr/bin/env bash
# Thin wrapper: stop the canonical PowerFS backend.
#
# P2 起主 compose 由 powerfs-ctl 渲染，停止路径统一走
# `powerfs-ctl down`（等同于在 rendered/ 目录下 docker compose down）。
#
# 用法：
#   ./docker/scripts/stop-cluster.sh             # 使用默认 $REPO_ROOT/.powerfs
#   POWERFS_HOME=/path/to/home ./docker/scripts/stop-cluster.sh
#   REMOVE_VOLUMES=1 ./docker/scripts/stop-cluster.sh   # 删数据卷
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOME_DIR="${POWERFS_HOME:-$REPO_ROOT/.powerfs}"
REMOVE_FLAG="${REMOVE_VOLUMES:+-v}"

if [ -f "$HOME_DIR/rendered/docker-compose.yml" ]; then
    cd "$HOME_DIR/rendered"
    docker compose down ${REMOVE_FLAG} --remove-orphans 2>&1 | tail -3
    echo "done (from $HOME_DIR/rendered)"
else
    # 兜底：无 render 产物时尝试按容器名停止（渲染项目名=cluster.name）
    if [ -f "$HOME_DIR/cluster.toml" ]; then
        CLUSTER_NAME=$(sed -n 's/^[[:space:]]*name[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$HOME_DIR/cluster.toml" | head -1)
        [ -n "$CLUSTER_NAME" ] && docker ps -q --filter "name=^${CLUSTER_NAME}-" | xargs -r docker stop
        echo "stopped by container name prefix (no rendered compose)"
    else
        echo "nothing to stop: no cluster.toml at $HOME_DIR" >&2
        exit 1
    fi
fi
