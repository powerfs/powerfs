#!/bin/bash
# =============================================================================
# 拓扑一致性检查：docker/topology.env  ↔  powerfs-ctl 渲染产物
#
# P2 起仓库内不再有手工维护的 compose：主拓扑由 `powerfs-ctl config render`
# 从 cluster.toml 生成。本脚本保证 legacy 证书夹具来源（topology.env）与
# powerfs-ctl init 模板默认拓扑不漂移（CI 中执行）：
#
#   TMP_HOME=$(mktemp -d)
#   powerfs-ctl --home "$TMP_HOME" init
#   powerfs-ctl --home "$TMP_HOME" config render
#   scripts/check-topology-sync.sh "$TMP_HOME/rendered/docker-compose.yml" \
#       "$TMP_HOME/cluster.toml"
#
# fuse 客户端不再是 compose 服务（见 docs/config-dehardcode-plan.md #3），
# 其 IP 从 cluster.toml 的 [client.*] 段提取后并入实际集合。
#
# 用法: scripts/check-topology-sync.sh [compose-file] [cluster-toml]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE="${1:-}"
CLUSTER_TOML="${2:-}"
TOPOLOGY_ENV="${TOPOLOGY_ENV:-${REPO_ROOT}/docker/topology.env}"

[ -f "$TOPOLOGY_ENV" ] || { echo "ERROR: missing $TOPOLOGY_ENV" >&2; exit 1; }
[ -n "$COMPOSE" ] || { echo "ERROR: usage: $0 <rendered-compose> [cluster.toml]" >&2; exit 1; }
[ -f "$COMPOSE" ] || { echo "ERROR: missing $COMPOSE (render it first: powerfs-ctl config render)" >&2; exit 1; }

# shellcheck source=../docker/topology.env
. "$TOPOLOGY_ENV"

fuse_expected=("${THREE_FUSE_IPS[@]}")
if [ -z "$CLUSTER_TOML" ]; then
    # 未提供 cluster.toml 时 fuse 客户端 IP 无从核对，从期望集合剔除并提示
    echo "NOTE: no cluster.toml given — skipping fuse client IP check" >&2
    fuse_expected=()
fi

expected="$(printf '%s\n' \
    "${THREE_MASTER_IPS[@]}" \
    "${THREE_VOLUME_IPS[@]}" \
    "${THREE_FILER_IPS[@]}" \
    "${fuse_expected[@]+"${fuse_expected[@]}"}" \
    "$REDIS_IP" "$MONITOR_IP" "$S3_IP" | sort -u)"

actual="$(grep -oE 'ipv4_address:[[:space:]]*[0-9]{1,3}(\.[0-9]{1,3}){3}' "$COMPOSE" \
    | awk -F: '{gsub(/[[:space:]]/, "", $2); print $2}')"

# fuse 客户端 IP 从 cluster.toml 的 [client.*] 段并入实际集合
if [ -n "$CLUSTER_TOML" ]; then
    [ -f "$CLUSTER_TOML" ] || { echo "ERROR: missing $CLUSTER_TOML" >&2; exit 1; }
    client_ips="$(awk '
        /^\[client\.[^]]+\]/ { in_client = 1; next }
        /^\[/ { in_client = 0 }
        in_client && /^[[:space:]]*ip[[:space:]]*=/ {
            v = $0; sub(/^[^"]*"/, "", v); sub(/".*$/, "", v); print v
        }
    ' "$CLUSTER_TOML")"
    actual="$(printf '%s\n%s\n' "$actual" "$client_ips")"
fi
actual="$(printf '%s\n' "$actual" | sort -u)"

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
