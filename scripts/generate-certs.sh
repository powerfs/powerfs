#!/usr/bin/env bash
# =============================================================================
# PowerFS TLS 证书生成脚本
# =============================================================================
# 通过 master CA 服务 (powerfs-cli cert) 生成完整证书套件：
#   - CA 证书 (ca.crt) — 分发给所有节点验证 leaf 证书
#   - 存储节点证书 (filer-*.crt, volume-*.crt) — filer/volume 作为
#     master 客户端 mTLS
#   - 客户端证书 (kernel-client-*.crt, fuse-client-*.crt) — FUSE/kernel 客户端
#     mount 时 mTLS，绑定源 IP + 挂载目录
#
# 前置条件:
#   1. Master 节点运行且 ca_dir 已配置 (见 docker/config/master.toml: ca_dir=...)
#      Master 首次启动会自动生成 CA (ca.crt + ca.key) 持久化到 ca_dir
#   2. powerfs-cli 已编译: cargo build --release -p powerfs-cli
#   3. 可达 master admin API (metrics_port, 默认 9300)
#
# 用法:
#   # Host RDMA 拓扑 (docker-compose.rdma.yml, host network 192.168.100.3)
#   ./scripts/generate-certs.sh
#
#   # 三节点 HA 拓扑 (docker-compose.yml, bridge 172.30.0.x)
#   ./scripts/generate-certs.sh --topology three --master-api 172.30.0.11:9300
#
#   # 生产环境 (输出到 gitignored docker/certs/, 不入库)
#   ./scripts/generate-certs.sh --output-dir docker/certs
#
# 缺省输出 docker/certs-default/ 提交到 git 作为开发/测试缺省证书。
# 生产部署请用 --output-dir docker/certs/ 重新生成, 并参考
# docker/certs-default/README.md 部署文档替换证书。
# =============================================================================
set -euo pipefail

# --- 拓扑单一来源（三层优先级）---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# 1. cluster.toml（declarative single source）—— 自动探测或 --home 指定
CLUSTER_TOML="${CLUSTER_TOML:-${POWERFS_HOME:-${REPO_ROOT}/.powerfs}/cluster.toml}"
RENDERED_COMPOSE="${RENDERED_COMPOSE:-${POWERFS_HOME:-${REPO_ROOT}/.powerfs}/rendered/docker-compose.yml}"
FROM_CLUSTER=0

# 2. docker/topology.env（legacy fallback，host RDMA 拓扑仍依赖）
TOPOLOGY_ENV="${TOPOLOGY_ENV:-${REPO_ROOT}/docker/topology.env}"
if [ -f "$TOPOLOGY_ENV" ]; then
    # shellcheck source=../docker/topology.env
    . "$TOPOLOGY_ENV"
else
    echo "WARN: $TOPOLOGY_ENV not found — falling back to built-in topology" >&2
fi

# 3. 决定是否启用 --from-cluster：
#    - 显式传 --from-cluster 强制启用
#    - cluster.toml + rendered compose 同时存在 **且** 用户未显式传
#      --topology / --master-api 时自动启用（避免抢 legacy 路径显式传参）
FROM_CLUSTER=0
AUTO_FROM_CLUSTER=0
if [ -f "$CLUSTER_TOML" ] && [ -f "$RENDERED_COMPOSE" ]; then
    AUTO_FROM_CLUSTER=1
fi
# 先扫描 $1..$N 是否有显式 --topology / --master-api
for a in "$@"; do
    case "$a" in --topology|--master-api) AUTO_FROM_CLUSTER=0; break;; esac
done

if [ "${1:-}" = "--from-cluster" ]; then
    FROM_CLUSTER=1; shift
elif [ "$AUTO_FROM_CLUSTER" = "1" ]; then
    FROM_CLUSTER=1
fi

die() { echo "ERROR: $*" >&2; exit 1; }

# --- 从 cluster.toml + rendered compose 解析节点与客户端 ---
# 产出：MASTER_API、ADMIN_TOKEN、MASTER_IDS/IPS、FILER_IDS/IPS、VOLUME_IDS/IPS、CLIENT_FUSE_IDS/IPS、kernel_client_ips
MASTER_IDS=(); MASTER_IPS=()
FILER_IDS=(); FILER_IPS=()
VOLUME_IDS=(); VOLUME_IPS=()
CLIENT_FUSE_IDS=(); CLIENT_FUSE_IPS=()
KERNEL_CLIENT_IDS=(); KERNEL_CLIENT_IPS=()

if [ "$FROM_CLUSTER" = "1" ]; then
    echo "INFO: --from-cluster mode — reading topology from $CLUSTER_TOML + $RENDERED_COMPOSE" >&2

    # ADMIN_TOKEN 优先 cluster.toml，其次 legacy env
    ADMIN_TOKEN="$(sed -n 's/^[[:space:]]*admin_token[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$CLUSTER_TOML" | head -1)"
    [ -n "$ADMIN_TOKEN" ] || ADMIN_TOKEN="${ADMIN_TOKEN:-powerfs-admin-test}"

    # admin_port：cluster.toml 没有单独字段，master admin API 模板默认 9300
    ADMIN_PORT=9300

    # --- 从 rendered compose 解析 node id→ip ---
    # rendered compose 结构：
    #   master-1:
    #     networks: { powerfs-network: { ipv4_address: 172.30.0.11 } }
    # awk 遇到 'role-N:' 行就记 current_role，随后第一个 ipv4_address 记到该 role
    declare -A NODE_IP_MAP
    while IFS='=' read -r role ip; do
        [ -z "$role" ] && continue
        NODE_IP_MAP["$role"]="$ip"
    done < <(awk '
        /^  [a-z]+-[0-9]+:[[:space:]]*$/ {
            role = $0; sub(/^  /, "", role); sub(/:[[:space:]]*$/, "", role)
            found_ip = 0
            next
        }
        role != "" && found_ip == 0 && /ipv4_address:/ {
            ip = $0; sub(/.*ipv4_address:[[:space:]]*/, "", ip); gsub(/[[:space:]]/, "", ip)
            found_ip = 1
            print role "=" ip
        }
    ' "$RENDERED_COMPOSE")

    # --- 按角色归档数组（数组有序：1, 2, 3, ...）---
    # master
    for key in $(printf '%s\n' "${!NODE_IP_MAP[@]}" | grep '^master-[0-9]\+$' | sort -V); do
        MASTER_IDS+=("$key"); MASTER_IPS+=("${NODE_IP_MAP[$key]}")
    done
    # filer
    for key in $(printf '%s\n' "${!NODE_IP_MAP[@]}" | grep '^filer-[0-9]\+$' | sort -V); do
        FILER_IDS+=("$key"); FILER_IPS+=("${NODE_IP_MAP[$key]}")
    done
    # volume
    for key in $(printf '%s\n' "${!NODE_IP_MAP[@]}" | grep '^volume-[0-9]\+$' | sort -V); do
        VOLUME_IDS+=("$key"); VOLUME_IPS+=("${NODE_IP_MAP[$key]}")
    done

    [ ${#MASTER_IDS[@]} -gt 0 ] || die "rendered compose 中未找到 master 节点（检查 $RENDERED_COMPOSE）"
    # MASTER_API = 首个 master IP + 9300
    MASTER_API="${MASTER_IPS[0]}:${ADMIN_PORT}"

    # --- 从 cluster.toml 解析 [client.*] ---
    # 复用 check-topology-sync.sh 已验证的 awk 模式
    while IFS='=' read -r ctype cname cip; do
        [ -z "$cname" ] && continue
        case "$ctype" in
            fuse)
                CLIENT_FUSE_IDS+=("$cname"); CLIENT_FUSE_IPS+=("$cip") ;;
            kernel)
                [ -n "$cip" ] && { KERNEL_CLIENT_IDS+=("$cname"); KERNEL_CLIENT_IPS+=("$cip"); } ;;
        esac
    done < <(awk '
        /^\[client\.[^]]+\]/ {
            if (name != "" && ctype != "") print ctype "=" name "=" ip
            name = $0; sub(/^\[client\./, "", name); sub(/\].*$/, "", name)
            ctype = ""; ip = ""; next
        }
        /^\[/ { if (name != "" && ctype != "") print ctype "=" name "=" ip; name = ""; ctype = ""; next }
        /^[[:space:]]*type[[:space:]]*=/ {
            v = $0; sub(/^[^"]*"/, "", v); sub(/".*$/, "", v); ctype = v; next
        }
        /^[[:space:]]*ip[[:space:]]*=/ {
            v = $0; sub(/^[^"]*"/, "", v); sub(/".*$/, "", v); ip = v; next
        }
        END { if (name != "" && ctype != "") print ctype "=" name "=" ip }
    ' "$CLUSTER_TOML")

    # --- 打印汇总 ---
    echo "  admin_token:    $ADMIN_TOKEN" >&2
    echo "  master_api:     $MASTER_API" >&2
    echo "  masters:        ${MASTER_IDS[*]} (${MASTER_IPS[*]})" >&2
    echo "  filers:         ${FILER_IDS[*]:-none}" >&2
    echo "  volumes:        ${VOLUME_IDS[*]:-none}" >&2
    echo "  fuse clients:    ${#CLIENT_FUSE_IDS[@]} (${CLIENT_FUSE_IDS[*]:-none})" >&2
    echo "  kernel clients:  ${#KERNEL_CLIENT_IDS[@]} (${KERNEL_CLIENT_IDS[*]:-none})" >&2
fi

# --- 缺省参数（legacy path 留原逻辑不动）---
MASTER_API="${MASTER_API:-${MASTER_API_HOST:-192.168.100.3}:${MASTER_API_PORT:-9300}}"
if [ "${ADMIN_TOKEN:-}" != "powerfs-admin-test" ] || [ "$FROM_CLUSTER" != "1" ]; then
    # 未从 cluster.toml 解析时用 legacy 默认（环境变量可覆盖）
    ADMIN_TOKEN="${ADMIN_TOKEN:-powerfs-admin-test}"
fi
OUTPUT_DIR="${OUTPUT_DIR:-docker/certs-default}"
TOPOLOGY="host"
MOUNT_DIR="${MOUNT_DIR:-/mnt/powerfs}"

# 自动探测 powerfs-cli (CLI 参数优先级最高, 然后是 repo-root/target/release/powerfs-cli,
# 然后是 CWD/target/release/powerfs-cli, 最后 PATH)
if [ -z "${CLI:-}" ]; then
    if [ -x "${REPO_ROOT}/target/release/powerfs-cli" ]; then
        CLI="${REPO_ROOT}/target/release/powerfs-cli"
    elif [ -x "target/release/powerfs-cli" ]; then
        CLI="target/release/powerfs-cli"
    elif command -v powerfs-cli &>/dev/null; then
        CLI="$(command -v powerfs-cli)"
    else
        CLI="target/release/powerfs-cli"  # 保留原缺省让后置 error 生效
    fi
fi

# 以下为 topology.env 缺失时的内置缺省；已从 env 加载的配置优先。
# host RDMA 拓扑: 所有存储节点共享 host IP 192.168.100.3
HOST_NODE_IP="${HOST_NODE_IP:-192.168.100.3}"
# VM kernel-client 源 IP (ib0 IPoIB 优先, eth0 docker-net 兜底)
declare -p HOST_VM1_IPS >/dev/null 2>&1 || HOST_VM1_IPS=(192.168.100.51 172.30.0.100)
declare -p HOST_VM2_IPS >/dev/null 2>&1 || HOST_VM2_IPS=(192.168.100.52 172.30.0.101)

# 三节点 HA: 各服务独立 IP (bridge 172.30.0.x)
# IP 必须与 docker-compose.yml 的 ipv4_address 一致, 否则 master 证书验证拒绝.
declare -p THREE_FILER_IDS  >/dev/null 2>&1 || THREE_FILER_IDS=(filer-1 filer-2 filer-3)
declare -p THREE_FILER_IPS  >/dev/null 2>&1 || THREE_FILER_IPS=(172.30.0.31 172.30.0.32 172.30.0.33)
declare -p THREE_VOLUME_IDS >/dev/null 2>&1 || THREE_VOLUME_IDS=(volume-1 volume-2 volume-3 volume-4 volume-5 volume-6)
declare -p THREE_VOLUME_IPS >/dev/null 2>&1 || THREE_VOLUME_IPS=(172.30.0.21 172.30.0.22 172.30.0.23 172.30.0.24 172.30.0.25 172.30.0.26)
declare -p THREE_FUSE_IDS   >/dev/null 2>&1 || THREE_FUSE_IDS=(fuse-client-1 fuse-client-2)
declare -p THREE_FUSE_IPS   >/dev/null 2>&1 || THREE_FUSE_IPS=(172.30.0.41 172.30.0.42)

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]
  --from-cluster            从 cluster.toml + rendered compose 读取拓扑
                            （自动：若 $CLUSTER_TOML 和 $RENDERED_COMPOSE 同时存在）
  --home DIR                等价 POWERFS_HOME=DIR
  --topology host|three     拓扑 (默认 host；--from-cluster 时自动忽略此选项)
  --master-api HOST:PORT    master admin API (默认 ${MASTER_API}；--from-cluster 时自动解析首 master)
  --admin-token TOKEN       admin token (默认 powerfs-admin-test；--from-cluster 时从 cluster.toml 读取)
  --output-dir DIR          输出目录 (默认 docker/certs-default)
  --mount-dir DIR           客户端绑定挂载目录 (默认 /mnt/powerfs)
  --cli PATH                powerfs-cli 路径 (默认 target/release/powerfs-cli)
  -h, --help                显示帮助

LEGACY：无 --from-cluster 时节点 IP 来自 docker/topology.env（或其内置缺省）。
host RDMA 拓扑始终走 legacy 路径（不读 cluster.toml）。
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --topology) TOPOLOGY="$2"; shift 2;;
        --master-api) MASTER_API="$2"; shift 2;;
        --admin-token) ADMIN_TOKEN="$2"; shift 2;;
        --output-dir) OUTPUT_DIR="$2"; shift 2;;
        --mount-dir) MOUNT_DIR="$2"; shift 2;;
        --cli) CLI="$2"; shift 2;;
        -h|--help) usage;;
        *) echo "Unknown option: $1" >&2; usage;;
    esac
done

# --- 前置检查 ---
if [[ ! -x "$CLI" ]]; then
    echo "ERROR: powerfs-cli not found at $CLI" >&2
    echo "  Run: cargo build --release -p powerfs-cli" >&2
    exit 1
fi

# 探测 master 是否可达
if ! curl -sk -m 3 -o /dev/null -w "" "https://${MASTER_API}/api/cert/ca" \
       -H "Authorization: Bearer ${ADMIN_TOKEN}" 2>/dev/null; then
    # /api/cert/ca 可能 401 (没 token) 但只要 TCP 通即可; 失败则 master 未启动
    if ! curl -sk -m 3 -o /dev/null "http://${MASTER_API}/" 2>/dev/null \
       && ! curl -sk -m 3 -o /dev/null "https://${MASTER_API}/" 2>/dev/null; then
        echo "ERROR: master admin API unreachable at ${MASTER_API}" >&2
        echo "  1. Ensure master running with ca_dir configured" >&2
        echo "  2. Check metrics_port in master.toml" >&2
        exit 1
    fi
fi

mkdir -p "$OUTPUT_DIR"
# 清理上轮生成的 leaf 证书 (ca.crt + *.crt + *.key).
# 保留 README.md / .gitkeep / client_registry.json (master 维护).
# 必要时 sudo: master 容器以 root 写入, 当前用户可能无权删除.
clean_file() {
    local f="$1"
    [[ -e "$f" ]] || return 0
    if ! rm -f "$f" 2>/dev/null; then
        echo "  (sudo rm $f — root-owned from previous master run)"
        sudo rm -f "$f"
    fi
}
clean_file "$OUTPUT_DIR/ca.crt"
shopt -s nullglob
for f in "$OUTPUT_DIR"/*.crt "$OUTPUT_DIR"/*.key; do
    [[ "$f" == *ca.crt || "$f" == *ca.key ]] && continue
    clean_file "$f"
done
shopt -u nullglob

echo "============================================================"
echo "Generating PowerFS TLS certificates"
echo "  topology:  $TOPOLOGY"
echo "  master:    $MASTER_API"
echo "  output:    $OUTPUT_DIR"
echo "  mount-dir: $MOUNT_DIR"
echo "============================================================"

# 辅助: 构造 --san-ip 参数串
san_ip_args() {
    local args=""
    for ip in "$@"; do
        args+=" --san-ip $ip"
    done
    echo "$args"
}

# --- 1. 获取 CA 证书 ---
echo
echo "=== [1/3] Fetching CA certificate ==="
"$CLI" cert init-ca --master-api "$MASTER_API" --admin-token "$ADMIN_TOKEN" -o "$OUTPUT_DIR"

# --- 2. 签发存储节点证书 ---
echo
echo "=== [2/3] Signing storage node certificates ==="
sign_node() {
    local node_id="$1" ip="$2"
    echo "  - $node_id (san-ip=$ip)"
    "$CLI" cert sign-node \
        --node-id "$node_id" \
        --master-api "$MASTER_API" \
        --admin-token "$ADMIN_TOKEN" \
        --san-ip "$ip" \
        -o "$OUTPUT_DIR"
}

if [ "$FROM_CLUSTER" = "1" ]; then
    # 从 cluster.toml + rendered compose 解析
    for i in "${!FILER_IDS[@]}"; do
        sign_node "${FILER_IDS[i]}" "${FILER_IPS[i]}"
    done
    for i in "${!VOLUME_IDS[@]}"; do
        sign_node "${VOLUME_IDS[i]}" "${VOLUME_IPS[i]}"
    done
else
    # legacy path（topology.env 或内置缺省）
    if [[ "$TOPOLOGY" == "host" ]]; then
        sign_node filer-1 "$HOST_NODE_IP"
        sign_node volume-1 "$HOST_NODE_IP"
        sign_node volume-2 "$HOST_NODE_IP"
        sign_node volume-3 "$HOST_NODE_IP"
    elif [[ "$TOPOLOGY" == "three" ]]; then
        for i in "${!THREE_FILER_IDS[@]}"; do
            sign_node "${THREE_FILER_IDS[i]}" "${THREE_FILER_IPS[i]}"
        done
        for i in "${!THREE_VOLUME_IDS[@]}"; do
            sign_node "${THREE_VOLUME_IDS[i]}" "${THREE_VOLUME_IPS[i]}"
        done
    else
        die "unknown topology '$TOPOLOGY' (use host|three)"
    fi
fi

# --- 3. 签发客户端证书 ---
echo
echo "=== [3/3] Signing client certificates ==="
sign_client() {
    local client_name="$1"; shift
    local -a ips=("$@")
    local args; args=$(san_ip_args "${ips[@]}")
    echo "  - $client_name (san-ips=${ips[*]})"
    # shellcheck disable=SC2086
    "$CLI" cert sign-client \
        --client-name "$client_name" \
        --master-api "$MASTER_API" \
        --admin-token "$ADMIN_TOKEN" \
        $args \
        --mount-dir "$MOUNT_DIR" \
        -o "$OUTPUT_DIR"
}

if [ "$FROM_CLUSTER" = "1" ]; then
    # 只签 cluster.toml [client.*] 声明了的客户端
    for i in "${!CLIENT_FUSE_IDS[@]}"; do
        sign_client "${CLIENT_FUSE_IDS[i]}" "${CLIENT_FUSE_IPS[i]}"
    done
    [ ${#CLIENT_FUSE_IDS[@]} -gt 0 ] || echo "  (no fuse clients declared in cluster.toml)" >&2
    for i in "${!KERNEL_CLIENT_IDS[@]}"; do
        # kernel-client 名从 cluster.toml 段名取（如 [client.kernel-default]）
        sign_client "${KERNEL_CLIENT_IDS[i]}" "${KERNEL_CLIENT_IPS[i]}"
    done
    [ ${#KERNEL_CLIENT_IDS[@]} -gt 0 ] || echo "  (no kernel clients declared in cluster.toml)" >&2
else
    if [[ "$TOPOLOGY" == "host" ]]; then
        sign_client kernel-client-1 "${HOST_VM1_IPS[@]}"
        sign_client kernel-client-2 "${HOST_VM2_IPS[@]}"
    elif [[ "$TOPOLOGY" == "three" ]]; then
        # kernel-client (VM, 假设 VM eth0 172.30.0.100/101)
        sign_client kernel-client-1 172.30.0.100
        sign_client kernel-client-2 172.30.0.101
        # fuse-client (FUSE 用户态, 各自 IP)
        for i in "${!THREE_FUSE_IDS[@]}"; do
            sign_client "${THREE_FUSE_IDS[i]}" "${THREE_FUSE_IPS[i]}"
        done
    fi
fi

# --- 汇总 ---
echo
echo "============================================================"
echo "Done. Generated certificates:"
ls -la "$OUTPUT_DIR"
echo "============================================================"
echo

# --- 完整性检查 (master ca_dir 挂载自 OUTPUT_DIR 时 master 自动写入) ---
check_file() {
    local f="$1"
    if [[ ! -s "$OUTPUT_DIR/$f" ]]; then
        echo "  WARN: $f missing or empty in $OUTPUT_DIR"
        return 1
    fi
    return 0
}

echo "完整性检查:"
MISSING=0
for f in ca.crt client_registry.json; do
    check_file "$f" || MISSING=$((MISSING+1))
done
# ca.key 仅在 OUTPUT_DIR == master ca_dir 挂载点时存在 (缺省入库场景)
# 生产模式 (OUTPUT_DIR=docker/certs) 不应有 ca.key, 该字段是 master 私钥.
if [[ "$OUTPUT_DIR" == *"certs-default"* ]]; then
    check_file "ca.key" || MISSING=$((MISSING+1))
fi
if [[ $MISSING -gt 0 ]]; then
    echo
    echo "  WARN: $MISSING file(s) missing — see above."
    echo "  If OUTPUT_DIR is NOT the master ca_dir mount point (e.g. docker/certs/),"
    echo "  ca.key + client_registry.json will not be present — that is expected for"
    echo "  production (master holds them internally on its own volume)."
fi

echo
echo "部署提示:"
echo "  - $OUTPUT_DIR/ca.crt              分发给所有节点 (验证 leaf 证书用)"
echo "  - 存储节点 (filer/volume): 部署 <node-id>.crt + <node-id>.key 到 /etc/powerfs/certs,"
echo "    filer.toml/volume.toml 已配置 ca_crt/client_crt/client_key"
echo "  - 客户端 (kernel/fuse): 部署 ca.crt + <client-name>.crt + <client-name>.key,"
echo "    mount 时通过 ca_crt/client_crt/client_key 选项指定"
echo
echo "缺省证书入库 (dev/test):"
echo "  - 当前 OUTPUT_DIR=$OUTPUT_DIR"
echo "  - 如果是 docker/certs-default/, 可直接 git add 入库"
echo "  - docker-compose 已挂载 ./certs-default 到 master /data/master/ca 和服务 /etc/powerfs/certs"
echo "  - 拉取代码即可 docker compose up, 缺省证书自动生效"
echo
echo "生产部署:"
echo "  - 用 --output-dir docker/certs 重新生成 (该目录被 .gitignore 忽略)"
echo "  - 生产证书不入库, 每个环境独立生成"
echo "  - admin_token 请从 master.toml 读取, 切勿使用缺省 token"
echo "  - 详细流程见 docker/certs-default/README.md"

# --- 权限修正: master 容器以 root 写入 ca.key + client_registry.json ---
# 若当前用户非 root, 提示 chown 以便 git add 入库.
if [[ "$(id -u)" -ne 0 ]]; then
    ROOT_OWNED=()
    while IFS= read -r f; do
        ROOT_OWNED+=("$f")
    done < <(find "$OUTPUT_DIR" -maxdepth 1 -user root -type f 2>/dev/null)
    if [[ ${#ROOT_OWNED[@]} -gt 0 ]]; then
        echo
        echo "============================================================"
        echo "权限修正 (master 容器以 root 写入以下文件):"
        printf '  %s\n' "${ROOT_OWNED[@]}"
        echo "  当前用户 $(whoami) 无法 git add 这些文件。"
        echo "  请执行:"
        echo "    sudo chown $(id -u):$(id -g) ${ROOT_OWNED[*]}"
        echo "  然后再 git add $OUTPUT_DIR/"
        echo "============================================================"
    fi
fi
