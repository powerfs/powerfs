#!/bin/bash
# =============================================================================
# 拓扑一致性检查：docker/topology.env  ↔  docker/docker-compose.yml
#
# P2 完成前 compose 中的 ipv4_address 仍为手写副本。本脚本保证两份数据一致，
# 避免改了一处漏了另一处（CI 中执行）。
#
# 用法: scripts/check-topology-sync.sh [compose-file]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE="${1:-${REPO_ROOT}/docker/docker-compose.yml}"
TOPOLOGY_ENV="${TOPOLOGY_ENV:-${REPO_ROOT}/docker/topology.env}"

[ -f "$TOPOLOGY_ENV" ] || { echo "ERROR: missing $TOPOLOGY_ENV" >&2; exit 1; }
[ -f "$COMPOSE" ] || { echo "ERROR: missing $COMPOSE" >&2; exit 1; }

# shellcheck source=../docker/topology.env
. "$TOPOLOGY_ENV"

expected="$(printf '%s\n' \
    "${THREE_MASTER_IPS[@]}" \
    "${THREE_VOLUME_IPS[@]}" \
    "${THREE_FILER_IPS[@]}" \
    "${THREE_FUSE_IPS[@]}" \
    "$REDIS_IP" "$MONITOR_IP" "$S3_IP" | sort -u)"

actual="$(grep -oE 'ipv4_address:[[:space:]]*[0-9]{1,3}(\.[0-9]{1,3}){3}' "$COMPOSE" \
    | awk -F: '{gsub(/[[:space:]]/, "", $2); print $2}' | sort -u)"

if [ "$expected" != "$actual" ]; then
    echo "ERROR: topology drift between $TOPOLOGY_ENV and $COMPOSE" >&2
    echo "--- expected (topology.env) ---" >&2
    echo "$expected" >&2
    echo "--- actual (compose ipv4_address) ---" >&2
    echo "$actual" >&2
    echo "--- diff (< expected, > actual) ---" >&2
    diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2 || true
    exit 1
fi

# 网段与网关也必须一致
compose_subnet="$(grep -oE 'subnet:[[:space:]]*[0-9./]+' "$COMPOSE" | head -1 | awk -F: '{gsub(/[[:space:]]/, "", $2); print $2}')"
compose_gateway="$(grep -oE 'gateway:[[:space:]]*[0-9.]+' "$COMPOSE" | head -1 | awk -F: '{gsub(/[[:space:]]/, "", $2); print $2}')"
[ "$compose_subnet" = "$SUBNET" ] || {
    echo "ERROR: subnet mismatch: topology.env=$SUBNET compose=$compose_subnet" >&2
    exit 1
}
[ "$compose_gateway" = "$GATEWAY" ] || {
    echo "ERROR: gateway mismatch: topology.env=$GATEWAY compose=$compose_gateway" >&2
    exit 1
}

echo "OK: topology in sync ($(printf '%s\n' "$expected" | wc -l) addresses, $SUBNET)"
