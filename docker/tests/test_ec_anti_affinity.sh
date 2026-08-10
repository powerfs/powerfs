#!/bin/bash
###############################################################################
# test_ec_anti_affinity.sh — PowerFS EC 分片节点级反亲和性验证测试
#
# 专门验证 P0 修复: EC 分片跨物理节点分布, 确保单节点故障不丢多分片.
#
# 测试场景:
#   1. 反亲和性验证: 6 分片分布在 6 个不同节点
#   2. 单节点故障降级读: 停 1 节点, 丢 1 分片, parity 重建成功
#   3. 双节点故障降级读: 停 2 节点 (不同节点), 丢 2 分片, 达容错上限
#   4. 三节点故障 EIO: 停 3 节点, 丢 3 分片 > 2 parity, 返回 EIO
#   5. 节点恢复: 重启后正常读
#   6. 多文件分布验证: 多个文件的分片均满足反亲和性
#
# 用法:
#   ./test_ec_anti_affinity.sh                  # 假设集群已运行
#   ./test_ec_anti_affinity.sh --start-cluster  # 先启动集群再测试
#   ./test_ec_anti_affinity.sh --cleanup        # 测试后关闭集群
#
# 环境变量:
#   EC_DATA_SHARDS     EC 数据分片数 (默认 4)
#   EC_PARITY_SHARDS   EC 校验分片数 (默认 2)
#   SCAN_INTERVAL      scrubber 扫描间隔秒 (默认 10)
###############################################################################

set -euo pipefail

# ============================================================================
# 配置
# ============================================================================
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DOCKER_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
PROJECT_DIR=$(cd "$DOCKER_DIR/.." && pwd)

EC_DATA_SHARDS="${EC_DATA_SHARDS:-4}"
EC_PARITY_SHARDS="${EC_PARITY_SHARDS:-2}"
TOTAL_SHARDS=$((EC_DATA_SHARDS + EC_PARITY_SHARDS))
SCAN_INTERVAL="${SCAN_INTERVAL:-10}"

FUSE_CONTAINER="fuse-1"
MOUNT_POINT="/mnt/powerfs"
TEST_DIR="${MOUNT_POINT}/ec_anti_affinity_test"
TESTUTIL="/app/powerfs-testutil"

# Filer 容器 (Raft 组)
FILERS=("filer-1" "filer-2" "filer-3")

# Volume 容器: 6 个节点, 每节点对应一个 volume 容器
VOLUMES=("volume-1" "volume-2" "volume-3" "volume-4" "volume-5" "volume-6")

# Volume IP 映射 (docker-compose.yml 中定义)
VOL_IPS=(
    "172.30.0.21"  # volume-1
    "172.30.0.22"  # volume-2
    "172.30.0.23"  # volume-3
    "172.30.0.24"  # volume-4
    "172.30.0.25"  # volume-5
    "172.30.0.26"  # volume-6
)

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# 测试计数
PASS=0
FAIL=0
FAILED_TESTS=()

# 参数
START_CLUSTER=false
CLEANUP=false

# ============================================================================
# 日志/测试辅助
# ============================================================================
log_info()  { echo -e "${BLUE}[INFO]${NC} $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $(date '+%H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*" >&2; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $(date '+%H:%M:%S') $*"; }
log_detail(){ echo -e "${CYAN}       ${NC} $*"; }

test_start() {
    echo ""
    echo "=============================================="
    echo "  TEST: $1"
    echo "=============================================="
}

test_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASS=$((PASS + 1))
}

test_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    echo "       Reason: $2"
    FAIL=$((FAIL + 1))
    FAILED_TESTS+=("$1: $2")
}

# ============================================================================
# FUSE 辅助
# ============================================================================

fuse_exec() {
    docker exec "$FUSE_CONTAINER" "$@"
}

wait_fuse_ready() {
    log_info "等待 FUSE 挂载就绪..."
    local retries=0
    while [ $retries -lt 60 ]; do
        if docker exec "$FUSE_CONTAINER" test -d "$MOUNT_POINT" 2>/dev/null && \
           docker exec "$FUSE_CONTAINER" mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
            if docker exec "$FUSE_CONTAINER" mkdir -p "$TEST_DIR" 2>/dev/null; then
                log_ok "FUSE 挂载就绪"
                return 0
            fi
        fi
        retries=$((retries + 1))
        sleep 2
    done
    log_error "FUSE 挂载超时"
    return 1
}

restart_fuse_clear_cache() {
    log_info "重启 $FUSE_CONTAINER 以清除 chunk_cache..."
    docker restart "$FUSE_CONTAINER" >/dev/null 2>&1
    wait_fuse_ready || { log_error "FUSE 重启后无法就绪"; return 1; }
    sleep 3
    log_ok "chunk_cache 已清除"
}

# ============================================================================
# 文件操作辅助
# ============================================================================

WRITE_FILE_PATH=""
WRITE_FILE_INODE=""
WRITE_FILE_SHA=""

write_test_file() {
    local size_mb="$1"
    local name="$2"
    WRITE_FILE_PATH="${TEST_DIR}/${name}_$(date +%s).dat"
    log_info "写入 ${size_mb}MB 测试文件: $WRITE_FILE_PATH"
    fuse_exec dd if=/dev/urandom of="$WRITE_FILE_PATH" bs=1M count="$size_mb" 2>&1 | tail -1
    fuse_exec sync
    WRITE_FILE_INODE=$(fuse_exec stat -c '%i' "$WRITE_FILE_PATH")
    WRITE_FILE_SHA=$(fuse_exec sha256sum "$WRITE_FILE_PATH" | awk '{print $1}')
    log_info "inode=$WRITE_FILE_INODE  sha256=${WRITE_FILE_SHA:0:32}..."
}

verify_sha256() {
    local file_path="$1"
    local expected_sha="$2"
    local actual_sha=""
    actual_sha=$(docker exec "$FUSE_CONTAINER" sha256sum "$file_path" 2>/dev/null | awk '{print $1}') || true
    if [ -z "$actual_sha" ]; then
        log_error "读取失败 (无法获取 checksum): $(basename "$file_path")"
        return 2
    fi
    if [ "$actual_sha" = "$expected_sha" ]; then
        log_ok "checksum 匹配: $(basename "$file_path")"
        return 0
    else
        log_error "checksum 不匹配: 期望 ${expected_sha:0:16}... 实际 ${actual_sha:0:16}..."
        return 1
    fi
}

# 验证读取返回 EIO (期望失败)
verify_read_eio() {
    local file_path="$1"
    local actual_sha=""
    actual_sha=$(docker exec "$FUSE_CONTAINER" sha256sum "$file_path" 2>&1) || true
    if echo "$actual_sha" | grep -qi "input/output error\|EIO\|Transport endpoint"; then
        log_ok "读取正确返回 EIO (期望行为, 3 分片丢失 > 2 parity 容错)"
        return 0
    fi
    if [ -n "$actual_sha" ] && ! echo "$actual_sha" | grep -q "No such file"; then
        # 如果 sha256sum 返回了有效 checksum, 说明不该成功时成功了
        log_error "读取意外成功 (期望 EIO): $actual_sha"
        return 1
    fi
    log_ok "读取返回错误 (期望 EIO)"
    return 0
}

# ============================================================================
# EC 转换辅助
# ============================================================================

SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

wait_ec_conversion() {
    local inode="$1"
    local timeout="${2:-180}"
    log_info "等待 inode $inode 的 EC 转换 (超时 ${timeout}s)..."

    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local log_line=""
        for filer in "${FILERS[@]}"; do
            log_line=$(docker logs "$filer" 2>&1 | grep "inode ${inode} EC converted" | tail -1 || true)
            [ -n "$log_line" ] && break
        done
        if [ -n "$log_line" ]; then
            log_ok "检测到 EC 转换完成"
            log_detail "$log_line"
            parse_g0_shards "$log_line"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
        if [ $((elapsed % 30)) -eq 0 ]; then
            log_warn "仍在等待... (${elapsed}s/$timeout)"
        fi
    done
    log_error "EC 转换超时 (${timeout}s)"
    for filer in "${FILERS[@]}"; do
        log_error "最近 $filer 日志:"
        docker logs "$filer" 2>&1 | tail -10 | sed 's/^/    /'
    done
    return 1
}

parse_g0_shards() {
    local log_line="$1"
    SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

    local shards_str="${log_line##*G0=\[}"
    shards_str="${shards_str%]*}"
    if [ -z "$shards_str" ]; then
        log_error "无法解析 G0 分片信息"
        return 1
    fi

    IFS=',' read -ra shard_arr <<< "$shards_str"
    for shard in "${shard_arr[@]}"; do
        shard="${shard#"${shard%%[![:space:]]*}"}"
        shard="${shard%"${shard##*[![:space:]]}"}"
        [ -z "$shard" ] && continue

        local kind idx vol needle addr
        kind="${shard:0:1}"
        idx="${shard#*[}"
        idx="${idx%%]*}"
        vol="${shard#*vol=}"
        vol="${vol%% *}"
        needle="${shard#*needle=}"
        needle="${needle%%@*}"
        addr="${shard##*@}"

        SHARD_KIND+=("$kind")
        SHARD_INDEX+=("$idx")
        SHARD_VOL+=("$vol")
        SHARD_NEEDLE+=("$needle")
        SHARD_ADDR+=("$addr")

        log_detail "G0 分片 ${kind}[${idx}]: vol=${vol} needle=${needle} addr=${addr}"
    done

    local total=${#SHARD_KIND[@]}
    local data_count=0 parity_count=0
    for k in "${SHARD_KIND[@]}"; do
        [ "$k" = "D" ] && data_count=$((data_count + 1))
        [ "$k" = "P" ] && parity_count=$((parity_count + 1))
    done
    log_ok "G0 共 ${total} 个分片 (${data_count} data + ${parity_count} parity)"

    if [ "$data_count" -ne "$EC_DATA_SHARDS" ] || [ "$parity_count" -ne "$EC_PARITY_SHARDS" ]; then
        log_error "分片数不匹配: 期望 ${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}, 实际 ${data_count}+${parity_count}"
        return 1
    fi
    return 0
}

# 获取 G0 数据分片的数组下标列表
get_data_shard_indices() {
    local indices=()
    for i in "${!SHARD_KIND[@]}"; do
        if [ "${SHARD_KIND[$i]}" = "D" ]; then
            indices+=("$i")
        fi
    done
    echo "${indices[@]}"
}

# ============================================================================
# 节点分布分析辅助
# ============================================================================

# 从分片地址 (172.30.0.21:8901) 提取 IP
shard_addr_to_ip() {
    local addr="$1"
    echo "${addr%%:*}"
}

# 从 IP 获取 volume 容器名
vol_ip_to_name() {
    local ip="$1"
    local last_octet="${ip##*.}"
    local vol_num=$((last_octet - 20))
    echo "volume-${vol_num}"
}

# 从分片在 SHARD_* 数组中的下标获取 volume 容器名
get_volume_for_shard() {
    local idx="$1"
    local addr="${SHARD_ADDR[$idx]}"
    local ip="${addr%%:*}"
    vol_ip_to_name "$ip"
}

# 打印 G0 分片的节点分布统计
print_shard_node_distribution() {
    echo ""
    echo "  ┌─────────────────────────────────────────────────┐"
    echo "  │  G0 分片节点分布                                 │"
    echo "  ├──────────┬──────────┬───────────────┬───────────┤"
    echo "  │ Shard    │ Volume   │ Node IP       │ Container │"
    echo "  ├──────────┼──────────┼───────────────┼───────────┤"

    local unique_ips=()
    for i in "${!SHARD_KIND[@]}"; do
        local kind="${SHARD_KIND[$i]}"
        local idx="${SHARD_INDEX[$i]}"
        local vol="${SHARD_VOL[$i]}"
        local addr="${SHARD_ADDR[$i]}"
        local ip=$(shard_addr_to_ip "$addr")
        local container=$(vol_ip_to_name "$ip")
        printf "  │ %s[%s]     │ %-8s │ %-13s │ %-9s │\n" "$kind" "$idx" "$vol" "$ip" "$container"

        # 收集唯一 IP
        local found=false
        for uip in "${unique_ips[@]+"${unique_ips[@]}"}"; do
            if [ "$uip" = "$ip" ]; then
                found=true
                break
            fi
        done
        if [ "$found" = false ]; then
            unique_ips+=("$ip")
        fi
    done

    echo "  ├──────────┴──────────┴───────────────┴───────────┤"
    echo "  │ 唯一节点数: ${#unique_ips[@]} / ${#SHARD_KIND[@]} 分片                      │"
    echo "  └─────────────────────────────────────────────────┘"

    # 返回唯一节点数
    echo "${#unique_ips[@]}"
}

# 验证 G0 分片的节点级反亲和性
# 参数: $1 = 期望的最小唯一节点数
# 返回: 0=通过, 1=失败
verify_node_anti_affinity() {
    local expected_min_nodes="$1"
    local unique_node_count

    # 获取所有分片的 IP
    local all_ips=()
    for i in "${!SHARD_ADDR[@]}"; do
        local ip=$(shard_addr_to_ip "${SHARD_ADDR[$i]}")
        all_ips+=("$ip")
    done

    # 计算唯一 IP 数
    local unique_ips=()
    for ip in "${all_ips[@]}"; do
        local found=false
        for uip in "${unique_ips[@]+"${unique_ips[@]}"}"; do
            if [ "$uip" = "$ip" ]; then
                found=true
                break
            fi
        done
        if [ "$found" = false ]; then
            unique_ips+=("$ip")
        fi
    done

    unique_node_count=${#unique_ips[@]}

    echo ""
    unique_node_count=$(print_shard_node_distribution | tail -1)

    log_info "唯一节点数: ${unique_node_count} (期望 >= ${expected_min_nodes})"

    if [ "$unique_node_count" -ge "$expected_min_nodes" ]; then
        log_ok "节点级反亲和性验证通过: ${unique_node_count} 个不同节点 >= ${expected_min_nodes}"
        return 0
    else
        log_error "节点级反亲和性验证失败: ${unique_node_count} 个节点 < ${expected_min_nodes}"
        log_error "分片集中在少数节点, 停 1 节点可能丢失多个分片"
        return 1
    fi
}

# ============================================================================
# 容器管理辅助
# ============================================================================

container_running() {
    docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -q true
}

stop_container() {
    local container="$1"
    log_info "停止容器: $container"
    docker stop "$container" >/dev/null 2>&1 || true
    log_ok "已停止: $container"
}

start_container() {
    local container="$1"
    log_info "启动容器: $container"
    docker start "$container" >/dev/null 2>&1 || true
    log_ok "已启动: $container"
}

wait_container_ready() {
    local container="$1"
    local type="${2:-volume}"
    local timeout="${3:-30}"
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if container_running "$container"; then
            if [ "$type" = "filer" ]; then
                log_info "等待 Raft 同步 (7s)..."
                sleep 7
            else
                log_info "等待 volume 注册到 master (7s)..."
                sleep 7
            fi
            log_ok "容器就绪: $container"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    log_error "容器 $container 启动超时"
    return 1
}

ensure_all_running() {
    log_info "确保所有容器运行中..."
    local started=0
    for c in "${FILERS[@]}" "${VOLUMES[@]}"; do
        if ! container_running "$c"; then
            log_warn "$c 未运行, 正在启动..."
            docker start "$c" >/dev/null 2>&1 || true
            started=$((started + 1))
        fi
    done
    if [ $started -gt 0 ]; then
        log_info "等待 ${started} 个容器恢复 (10s)..."
        sleep 10
    fi
    log_ok "所有容器已确认运行"
}

# ============================================================================
# 集群管理
# ============================================================================

start_cluster() {
    log_info "启动集群 (EC=${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}, scan=${SCAN_INTERVAL}s)..."
    export POWERFS_EC_DATA_SHARDS="$EC_DATA_SHARDS"
    export POWERFS_EC_PARITY_SHARDS="$EC_PARITY_SHARDS"
    export POWERFS_SCRUBBER_SCAN_INTERVAL="$SCAN_INTERVAL"
    export POWERFS_EC_MIN_FILE_SIZE=0
    export POWERFS_SCRUBBER_MAX_INODES=50

    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
    bash "$DOCKER_DIR/scripts/start-cluster.sh" 2>&1 | tail -30

    log_info "启动 volume-4/5/6..."
    docker compose up -d --no-deps volume-4 volume-5 volume-6 2>&1 || true

    local retries=0
    while [ $retries -lt 30 ]; do
        local ready=0
        for vol in volume-4 volume-5 volume-6; do
            container_running "$vol" && ready=$((ready + 1))
        done
        if [ $ready -eq 3 ]; then
            break
        fi
        retries=$((retries + 1))
        sleep 2
    done
    sleep 5

    wait_fuse_ready
}

stop_cluster() {
    log_info "停止集群..."
    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
    log_ok "集群已停止"
}

# ============================================================================
# 测试场景 1: 反亲和性验证 — 6 分片分布在 6 个不同节点
# ============================================================================
test_anti_affinity_distribution() {
    test_start "场景 1: 反亲和性验证 (6 分片 → 6 不同节点)"

    write_test_file 8 "anti_affinity_dist"
    local file_path="$WRITE_FILE_PATH"
    local file_sha="$WRITE_FILE_SHA"
    local file_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file_inode" 180; then
        test_fail "场景1-EC转换" "EC 转换超时"
        return 1
    fi
    test_pass "场景1-EC转换" "EC 转换完成"

    # 验证节点级反亲和性: 6 分片应分布在 6 个不同节点
    if verify_node_anti_affinity "$TOTAL_SHARDS"; then
        test_pass "场景1-反亲和性" "${TOTAL_SHARDS} 个分片分布在 ${TOTAL_SHARDS} 个不同节点 (完美反亲和)"
    else
        test_fail "场景1-反亲和性" "分片未分布在足够多的节点上"
        fuse_exec rm -f "$file_path" 2>/dev/null || true
        return 1
    fi

    # 验证文件可正常读取 (重启 FUSE 清除缓存, 确保 EC 路径读取)
    restart_fuse_clear_cache || {
        test_fail "场景1-正常读" "FUSE 重启失败"
        fuse_exec rm -f "$file_path" 2>/dev/null || true
        return 1
    }
    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景1-正常读" "所有分片完好时读取成功"
    else
        test_fail "场景1-正常读" "正常读取失败"
        fuse_exec rm -f "$file_path" 2>/dev/null || true
        return 1
    fi

    fuse_exec rm -f "$file_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 2: 单节点故障降级读 — 停 1 节点, 丢 1 分片
# ============================================================================
test_single_node_failure() {
    test_start "场景 2: 单节点故障降级读 (停 1 节点 → 丢 1 分片)"

    write_test_file 8 "single_node_fail"
    local file_path="$WRITE_FILE_PATH"
    local file_sha="$WRITE_FILE_SHA"
    local file_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file_inode" 180; then
        test_fail "场景2-EC转换" "EC 转换超时"
        return 1
    fi
    test_pass "场景2-EC转换" "EC 转换完成"

    # 验证反亲和性 (前提条件)
    if ! verify_node_anti_affinity "$TOTAL_SHARDS"; then
        test_fail "场景2-前提" "反亲和性不满足, 无法保证停 1 节点只丢 1 分片"
        fuse_exec rm -f "$file_path" 2>/dev/null || true
        return 1
    fi

    # 找到 G0 D[0] 所在的 volume 容器
    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 1 ]; then
        test_fail "场景2" "无数据分片可定位"
        return 1
    fi

    local target_vol
    target_vol=$(get_volume_for_shard "${data_indices[0]}")
    log_info "G0 D[0] 位于 $target_vol, 停止该节点模拟 1 分片丢失"

    # 停止该 volume 节点
    stop_container "$target_vol"

    # 重启 fuse 清除缓存, 验证降级读
    restart_fuse_clear_cache || {
        test_fail "场景2" "FUSE 重启失败"
        start_container "$target_vol"
        wait_container_ready "$target_vol" volume || true
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景2-降级读" "1 节点停机后 EC 降级读成功 (1 分片丢失, parity 重建)"
    else
        test_fail "场景2-降级读" "EC 降级读失败 (1 分片丢失应可恢复)"
        start_container "$target_vol"
        wait_container_ready "$target_vol" volume || true
        return 1
    fi

    # 恢复节点
    start_container "$target_vol"
    wait_container_ready "$target_vol" volume || true

    restart_fuse_clear_cache || {
        test_fail "场景2" "FUSE 重启失败"
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景2-恢复" "节点恢复后正常读成功"
    else
        test_fail "场景2-恢复" "恢复后读取失败"
        return 1
    fi

    fuse_exec rm -f "$file_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 3: 双节点故障降级读 — 停 2 不同节点, 丢 2 分片 (容错上限)
# ============================================================================
test_dual_node_failure() {
    test_start "场景 3: 双节点故障降级读 (停 2 节点 → 丢 2 分片, 容错上限)"

    write_test_file 8 "dual_node_fail"
    local file_path="$WRITE_FILE_PATH"
    local file_sha="$WRITE_FILE_SHA"
    local file_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file_inode" 180; then
        test_fail "场景3-EC转换" "EC 转换超时"
        return 1
    fi
    test_pass "场景3-EC转换" "EC 转换完成"

    # 验证反亲和性 (前提条件)
    if ! verify_node_anti_affinity "$TOTAL_SHARDS"; then
        test_fail "场景3-前提" "反亲和性不满足"
        fuse_exec rm -f "$file_path" 2>/dev/null || true
        return 1
    fi

    # 找到 G0 D[0] 和 D[1] 所在的不同 volume 容器
    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 2 ]; then
        test_fail "场景3" "数据分片不足 2 个"
        return 1
    fi

    local vol1 vol2
    vol1=$(get_volume_for_shard "${data_indices[0]}")
    # 找一个与 vol1 不同的 volume (确保在 不同节点)
    vol2=""
    for i in "${data_indices[@]:1}"; do
        local v
        v=$(get_volume_for_shard "$i")
        if [ "$v" != "$vol1" ]; then
            vol2="$v"
            break
        fi
    done
    if [ -z "$vol2" ]; then
        log_warn "所有数据分片位于同一节点, 使用第二个分片所在 volume"
        vol2=$(get_volume_for_shard "${data_indices[1]}")
    fi

    log_info "G0 D[0] 位于 $vol1, D[1] 位于 $vol2, 同时停止两个不同节点"

    # 同时停止两个 volume 节点
    stop_container "$vol1"
    stop_container "$vol2"

    # 重启 fuse, 验证降级读 (2 分片丢失 = EC(4+2) 容错上限)
    restart_fuse_clear_cache || {
        test_fail "场景3" "FUSE 重启失败"
        start_container "$vol1"
        start_container "$vol2"
        wait_container_ready "$vol1" volume || true
        wait_container_ready "$vol2" volume || true
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景3-降级读" "2 节点停机后 EC 降级读成功 (2 分片丢失 = 容错上限, 2 parity 重建)"
    else
        test_fail "场景3-降级读" "EC 降级读失败 (2 分片丢失在容错范围内)"
        start_container "$vol1"
        start_container "$vol2"
        wait_container_ready "$vol1" volume || true
        wait_container_ready "$vol2" volume || true
        return 1
    fi

    # 恢复两个节点
    start_container "$vol1"
    start_container "$vol2"
    wait_container_ready "$vol1" volume || true
    wait_container_ready "$vol2" volume || true

    restart_fuse_clear_cache || {
        test_fail "场景3" "FUSE 重启失败"
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景3-恢复" "两个节点恢复后正常读成功"
    else
        test_fail "场景3-恢复" "恢复后读取失败"
        return 1
    fi

    fuse_exec rm -f "$file_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 4: 三节点故障 EIO — 停 3 不同节点, 丢 3 分片 > 2 parity
# ============================================================================
test_triple_node_failure_eio() {
    test_start "场景 4: 三节点故障 EIO (停 3 节点 → 丢 3 分片 > 2 parity)"

    write_test_file 8 "triple_node_fail"
    local file_path="$WRITE_FILE_PATH"
    local file_sha="$WRITE_FILE_SHA"
    local file_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file_inode" 180; then
        test_fail "场景4-EC转换" "EC 转换超时"
        return 1
    fi
    test_pass "场景4-EC转换" "EC 转换完成"

    # 验证反亲和性 (前提条件)
    if ! verify_node_anti_affinity "$TOTAL_SHARDS"; then
        test_fail "场景4-前提" "反亲和性不满足"
        fuse_exec rm -f "$file_path" 2>/dev/null || true
        return 1
    fi

    # 找到 3 个不同节点上的分片
    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 3 ]; then
        test_fail "场景4" "数据分片不足 3 个"
        return 1
    fi

    # 收集前 3 个不同节点的 volume
    local stopped_vols=()
    local stopped_ips=()
    for i in "${data_indices[@]}"; do
        local v
        v=$(get_volume_for_shard "$i")
        local ip
        ip=$(shard_addr_to_ip "${SHARD_ADDR[$i]}")

        # 检查是否已经在停止列表中 (不同节点)
        local already=false
        for sip in "${stopped_ips[@]+"${stopped_ips[@]}"}"; do
            if [ "$sip" = "$ip" ]; then
                already=true
                break
            fi
        done

        if [ "$already" = false ]; then
            stopped_vols+=("$v")
            stopped_ips+=("$ip")
        fi

        if [ ${#stopped_vols[@]} -eq 3 ]; then
            break
        fi
    done

    if [ ${#stopped_vols[@]} -lt 3 ]; then
        log_warn "只找到 ${#stopped_vols[@]} 个不同节点的分片 (需要 3 个)"
        test_fail "场景4" "无法找到 3 个不同节点 (反亲和性不足)"
        fuse_exec rm -f "$file_path" 2>/dev/null || true
        return 1
    fi

    log_info "停止 3 个不同节点: ${stopped_vols[*]} (IPs: ${stopped_ips[*]})"

    # 停止 3 个 volume 节点
    for v in "${stopped_vols[@]}"; do
        stop_container "$v"
    done

    # 重启 fuse, 验证 EIO (3 分片丢失 > 2 parity 容错)
    restart_fuse_clear_cache || {
        test_fail "场景4" "FUSE 重启失败"
        for v in "${stopped_vols[@]}"; do
            start_container "$v"
        done
        for v in "${stopped_vols[@]}"; do
            wait_container_ready "$v" volume || true
        done
        return 1
    }

    if verify_read_eio "$file_path"; then
        test_pass "场景4-EIO" "3 节点停机后正确返回 EIO (3 分片丢失 > 2 parity 容错上限)"
    else
        test_fail "场景4-EIO" "读取未返回 EIO (期望失败)"
    fi

    # 恢复所有节点
    for v in "${stopped_vols[@]}"; do
        start_container "$v"
    done
    for v in "${stopped_vols[@]}"; do
        wait_container_ready "$v" volume || true
    done

    # 恢复后验证正常读
    restart_fuse_clear_cache || {
        test_fail "场景4" "FUSE 重启失败"
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景4-恢复" "3 节点恢复后正常读成功"
    else
        test_fail "场景4-恢复" "恢复后读取失败"
    fi

    fuse_exec rm -f "$file_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 5: 节点恢复后正常读
# ============================================================================
test_node_recovery() {
    test_start "场景 5: 节点恢复后正常读 (故障 → 恢复 → 验证)"

    write_test_file 8 "node_recovery"
    local file_path="$WRITE_FILE_PATH"
    local file_sha="$WRITE_FILE_SHA"
    local file_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file_inode" 180; then
        test_fail "场景5-EC转换" "EC 转换超时"
        return 1
    fi
    test_pass "场景5-EC转换" "EC 转换完成"

    # 验证反亲和性
    if ! verify_node_anti_affinity "$TOTAL_SHARDS"; then
        test_fail "场景5-前提" "反亲和性不满足"
        fuse_exec rm -f "$file_path" 2>/dev/null || true
        return 1
    fi

    # 找到 D[0] 所在节点, 停止 → 验证降级读 → 恢复 → 验证正常读
    local data_indices
    data_indices=($(get_data_shard_indices))
    local target_vol
    target_vol=$(get_volume_for_shard "${data_indices[0]}")

    # Step 1: 正常读 (基线)
    restart_fuse_clear_cache || { test_fail "场景5" "FUSE 重启失败"; return 1; }
    if verify_sha256 "$file_path" "$file_sha"; then
        log_ok "基线: 正常读取成功"
    else
        test_fail "场景5-基线" "基线读取失败"
        return 1
    fi

    # Step 2: 停止节点 → 降级读
    stop_container "$target_vol"
    restart_fuse_clear_cache || {
        test_fail "场景5" "FUSE 重启失败"
        start_container "$target_vol"
        wait_container_ready "$target_vol" volume || true
        return 1
    }
    if verify_sha256 "$file_path" "$file_sha"; then
        log_ok "降级读: 1 节点停机后读取成功"
    else
        test_fail "场景5-降级读" "降级读失败"
        start_container "$target_vol"
        wait_container_ready "$target_vol" volume || true
        return 1
    fi

    # Step 3: 恢复节点 → 正常读
    start_container "$target_vol"
    wait_container_ready "$target_vol" volume || true
    restart_fuse_clear_cache || { test_fail "场景5" "FUSE 重启失败"; return 1; }
    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景5-恢复读" "节点恢复后正常读成功 (故障→降级→恢复全链路验证)"
    else
        test_fail "场景5-恢复读" "恢复后读取失败"
        return 1
    fi

    fuse_exec rm -f "$file_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 6: 多文件分布验证 — 每个文件都满足反亲和性
# ============================================================================
test_multi_file_distribution() {
    test_start "场景 6: 多文件分布验证 (3 文件, 每个均满足反亲和性)"

    local files=()
    local shas=()
    local inodes=()
    local all_pass=true
    local i

    # 写入 3 个文件, 每个等待 EC 转换
    for i in 1 2 3; do
        write_test_file 8 "multi_dist_$i"
        files+=("$WRITE_FILE_PATH")
        shas+=("$WRITE_FILE_SHA")
        inodes+=("$WRITE_FILE_INODE")
        if ! wait_ec_conversion "$WRITE_FILE_INODE" 180; then
            test_fail "场景6-EC转换" "文件 $i EC 转换超时"
            for f in "${files[@]}"; do
                fuse_exec rm -f "$f" 2>/dev/null || true
            done
            return 1
        fi
    done
    test_pass "场景6-EC转换" "3 个文件全部 EC 转换完成"

    # 逐个验证反亲和性
    for i in "${!inodes[@]}"; do
        log_info "--- 文件 $((i + 1))/3 反亲和性验证 ---"

        # 重新解析该文件的 G0 分片 (需要从日志重新获取)
        local log_line=""
        for filer in "${FILERS[@]}"; do
            log_line=$(docker logs "$filer" 2>&1 | grep "inode ${inodes[$i]} EC converted" | tail -1 || true)
            [ -n "$log_line" ] && break
        done

        if [ -z "$log_line" ]; then
            log_error "无法找到文件 $((i + 1)) 的 EC 转换日志"
            all_pass=false
            continue
        fi

        parse_g0_shards "$log_line" || { all_pass=false; continue; }

        if verify_node_anti_affinity "$TOTAL_SHARDS"; then
            log_ok "文件 $((i + 1)): 反亲和性验证通过"
        else
            log_error "文件 $((i + 1)): 反亲和性验证失败"
            all_pass=false
        fi
    done

    if $all_pass; then
        test_pass "场景6-多文件反亲和" "3 个文件均满足节点级反亲和性 (每个文件 6 分片在 6 不同节点)"
    else
        test_fail "场景6-多文件反亲和" "部分文件反亲和性不满足"
    fi

    # 验证所有文件可正常读取
    restart_fuse_clear_cache || {
        test_fail "场景6" "FUSE 重启失败"
        for f in "${files[@]}"; do
            fuse_exec rm -f "$f" 2>/dev/null || true
        done
        return 1
    }

    local read_ok=true
    for i in "${!files[@]}"; do
        verify_sha256 "${files[$i]}" "${shas[$i]}" || read_ok=false
    done

    if $read_ok; then
        test_pass "场景6-正常读" "3 个文件全部读取成功"
    else
        test_fail "场景6-正常读" "部分文件读取失败"
    fi

    for f in "${files[@]}"; do
        fuse_exec rm -f "$f" 2>/dev/null || true
    done
    return 0
}

# ============================================================================
# 参数解析
# ============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --start-cluster) START_CLUSTER=true; shift ;;
            --cleanup)       CLEANUP=true; shift ;;
            --help|-h)
                head -30 "$0"
                exit 0
                ;;
            *) shift ;;
        esac
    done
}

# ============================================================================
# 主流程
# ============================================================================
main() {
    parse_args "$@"

    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  PowerFS EC 分片节点级反亲和性验证测试                    ║"
    echo "║  EC=${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}  scan=${SCAN_INTERVAL}s  volumes=${#VOLUMES[@]}                ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""

    # 启动集群
    if [ "$START_CLUSTER" = true ]; then
        start_cluster
    else
        if ! container_running "$FUSE_CONTAINER"; then
            log_error "$FUSE_CONTAINER 未运行, 请用 --start-cluster 启动集群"
            exit 1
        fi
        wait_fuse_ready
    fi

    # 确认 testutil 可用
    if ! docker exec "$FUSE_CONTAINER" test -f "$TESTUTIL" 2>/dev/null; then
        log_warn "$TESTUTIL 不存在于 $FUSE_CONTAINER (诊断功能不可用, 测试仍可继续)"
    else
        log_ok "powerfs-testutil 可用"
    fi

    # 确认 volume 数量
    local volume_count=0
    for v in "${VOLUMES[@]}"; do
        container_running "$v" && volume_count=$((volume_count + 1))
    done
    log_info "运行中的 volume 容器: ${volume_count}/${#VOLUMES[@]}, EC 需要: ${TOTAL_SHARDS}"
    if [ "$volume_count" -lt "$TOTAL_SHARDS" ]; then
        log_error "volume 数量不足: ${volume_count} < ${TOTAL_SHARDS}"
        exit 1
    fi

    # 确认 filer 数量
    local filer_count=0
    for f in "${FILERS[@]}"; do
        container_running "$f" && filer_count=$((filer_count + 1))
    done
    log_info "运行中的 filer 容器: ${filer_count}/${#FILERS[@]}"
    if [ "$filer_count" -lt 2 ]; then
        log_error "filer 数量不足: ${filer_count} < 2 (Raft 需要多数派)"
        exit 1
    fi

    # 执行所有测试
    log_info "开始执行 6 个测试场景..."

    test_anti_affinity_distribution || true
    ensure_all_running

    test_single_node_failure || true
    ensure_all_running

    test_dual_node_failure || true
    ensure_all_running

    test_triple_node_failure_eio || true
    ensure_all_running

    test_node_recovery || true
    ensure_all_running

    test_multi_file_distribution || true
    ensure_all_running

    # 清理测试目录
    log_info "清理测试文件..."
    fuse_exec rm -rf "$TEST_DIR" 2>/dev/null || true

    # 汇总
    echo ""
    echo "=============================================="
    echo "  测试汇总"
    echo "=============================================="
    echo -e "  ${GREEN}通过: $PASS${NC}"
    echo -e "  ${RED}失败: $FAIL${NC}"
    if [ "$FAIL" -gt 0 ]; then
        echo ""
        echo "失败用例:"
        for failed in "${FAILED_TESTS[@]+"${FAILED_TESTS[@]}"}"; do
            echo "  - $failed"
        done
    fi
    echo ""

    # 关闭集群
    if [ "$CLEANUP" = true ]; then
        stop_cluster
    fi

    if [ "$FAIL" -gt 0 ]; then
        exit 1
    fi
    exit 0
}

main "$@"
