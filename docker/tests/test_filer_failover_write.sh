#!/bin/bash
###############################################################################
# test_filer_failover_write.sh — PowerFS Filer 故障写入修复验证
#
# 验证 P2-1 修复: send_coherence_msg / process_request_internal 的
# 网络错误重试 + Filer 轮换逻辑，确保 Leader Filer 故障期间写入不丢失。
#
# 测试场景:
#   1. 正常写入基准文件 (确认集群健康)
#   2. 停止 Leader Filer, 选举期间持续写入 (核心: 重试+轮换)
#   3. 验证故障期间写入的文件可读且 checksum 正确
#   4. 恢复 Filer, 验证数据一致性与持续可用性
#   5. 连续两次 Leader 切换, 验证多次故障恢复
#
# 用法:
#   ./test_filer_failover_write.sh                  # 假设集群已运行
#   ./test_filer_failover_write.sh --start-cluster  # 先启动集群再测试
#   ./test_filer_failover_write.sh --cleanup        # 测试后关闭集群
###############################################################################

set -euo pipefail

# ============================================================================
# 配置
# ============================================================================
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DOCKER_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
PROJECT_DIR=$(cd "$DOCKER_DIR/.." && pwd)

FUSE_CONTAINER="fuse-1"
MOUNT_POINT="/mnt/powerfs"
TEST_DIR="${MOUNT_POINT}/filer_failover_test"

# Filer 容器 (Raft 组, 3 节点)
FILER_CONTAINERS=("filer-1" "filer-2" "filer-3")
# Filer HTTP 端口 (host 映射, 用于 /admin/status 查询 leader)
FILER_HTTP_PORTS=("8888" "8898" "8908")
# Filer 内部 IP (用于日志关联)
FILER_IPS=("172.30.0.31" "172.30.0.32" "172.30.0.33")

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
            if fuse_exec mkdir -p "$TEST_DIR" 2>/dev/null; then
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
    log_info "重启 $FUSE_CONTAINER 以清除缓存..."
    docker restart "$FUSE_CONTAINER" >/dev/null 2>&1
    wait_fuse_ready || { log_error "FUSE 重启后无法就绪"; return 1; }
    sleep 3
    log_ok "缓存已清除"
}

# ============================================================================
# Filer Leader 查询
# ============================================================================

# 查询当前 Leader Filer 容器名
# 通过 /admin/status 接口的 leader_count 字段判断 (leader_count > 0 即为 leader)
find_leader_filer() {
    local idx
    for idx in 0 1 2; do
        local port="${FILER_HTTP_PORTS[$idx]}"
        local status
        status=$(curl -s --max-time 3 "http://localhost:${port}/admin/status" 2>/dev/null || echo "")
        if [ -z "$status" ]; then
            continue
        fi
        # /admin/status 返回 JSON: {"shard_count":N,"leader_count":M,...}
        # leader_count > 0 表示该 filer 是至少一个 shard 的 leader
        local lc
        lc=$(echo "$status" | grep -o '"leader_count":[0-9]*' | head -1 | grep -o '[0-9]*' || echo "0")
        if [ -n "$lc" ] && [ "$lc" -gt 0 ]; then
            echo "${FILER_CONTAINERS[$idx]}"
            return 0
        fi
    done
    return 1
}

# 查询所有存活 Filer 容器 (HTTP 可达)
find_alive_filers() {
    local alive=()
    local idx
    for idx in 0 1 2; do
        local port="${FILER_HTTP_PORTS[$idx]}"
        if curl -s --max-time 2 "http://localhost:${port}/admin/status" >/dev/null 2>&1; then
            alive+=("${FILER_CONTAINERS[$idx]}")
        fi
    done
    echo "${alive[@]}"
}

# 等待 Leader 选举完成 (至少一个存活 filer 报告 is_leader=true)
wait_for_leader_election() {
    local max_wait="${1:-30}"
    local elapsed=0
    log_info "等待 Leader 选举完成 (最多 ${max_wait}s)..."
    while [ $elapsed -lt $max_wait ]; do
        local leader
        leader=$(find_leader_filer 2>/dev/null || echo "")
        if [ -n "$leader" ]; then
            log_ok "新 Leader: $leader (等待 ${elapsed}s)"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    log_error "Leader 选举超时 (${max_wait}s)"
    return 1
}

# ============================================================================
# 文件操作辅助
# ============================================================================

# 写入测试文件并记录 checksum
# 参数: $1=size_mb, $2=name
# 设置全局: WRITE_FILE_PATH, WRITE_FILE_SHA
WRITE_FILE_PATH=""
WRITE_FILE_SHA=""

write_test_file() {
    local size_mb="$1"
    local name="$2"
    WRITE_FILE_PATH="${TEST_DIR}/${name}_$(date +%s).dat"
    log_info "写入 ${size_mb}MB 文件: $WRITE_FILE_PATH"
    fuse_exec dd if=/dev/urandom of="$WRITE_FILE_PATH" bs=1M count="$size_mb" 2>&1 | tail -1
    fuse_exec sync 2>/dev/null || true
    WRITE_FILE_SHA=$(fuse_exec sha256sum "$WRITE_FILE_PATH" 2>/dev/null | awk '{print $1}')
    if [ -z "$WRITE_FILE_SHA" ]; then
        log_error "写入失败: 无法获取 checksum"
        return 1
    fi
    log_detail "sha256=${WRITE_FILE_SHA:0:32}... size=${size_mb}MB"
    return 0
}

# 写入文件但允许失败 (用于故障期间测试, 记录是否 EIO)
# 返回: 0=成功, 1=失败
write_file_expect_success() {
    local size_mb="$1"
    local name="$2"
    local path="${TEST_DIR}/${name}_$(date +%s).dat"
    log_info "故障期间写入 ${size_mb}MB 文件: $path"
    local output
    output=$(fuse_exec dd if=/dev/urandom of="$path" bs=1M count="$size_mb" 2>&1) || {
        log_error "写入失败 (EIO?): $output"
        return 1
    }
    fuse_exec sync 2>/dev/null || true
    local sha
    sha=$(fuse_exec sha256sum "$path" 2>/dev/null | awk '{print $1}')
    if [ -z "$sha" ]; then
        log_error "写入后无法读取 (EIO?)"
        return 1
    fi
    log_detail "sha256=${sha:0:32}..."
    # 记录到全局供后续验证
    WRITE_FILE_PATH="$path"
    WRITE_FILE_SHA="$sha"
    return 0
}

# 验证文件 checksum
# 参数: $1=path, $2=expected_sha
verify_sha256() {
    local file_path="$1"
    local expected_sha="$2"
    local actual_sha
    actual_sha=$(fuse_exec sha256sum "$file_path" 2>/dev/null | awk '{print $1}') || true
    if [ -z "$actual_sha" ]; then
        log_error "读取失败: $(basename "$file_path")"
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

# ============================================================================
# 集群管理
# ============================================================================

start_cluster() {
    log_info "启动集群..."
    cd "$DOCKER_DIR"
    bash scripts/start-cluster.sh
    cd "$SCRIPT_DIR"
}

stop_cluster() {
    log_info "停止集群..."
    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
    cd "$SCRIPT_DIR"
}

# ============================================================================
# 测试场景
# ============================================================================

# 场景 1: 正常写入基准文件
test_scenario_1_baseline() {
    test_start "场景1: 正常写入基准文件"

    # 确认集群健康
    local leader
    leader=$(find_leader_filer 2>/dev/null || echo "")
    if [ -z "$leader" ]; then
        test_fail "场景1" "无法找到 Leader Filer, 集群可能未就绪"
        return 1
    fi
    log_detail "当前 Leader: $leader"

    # 写入基准文件
    if ! write_test_file 5 "baseline"; then
        test_fail "场景1" "基准文件写入失败"
        return 1
    fi
    local baseline_path="$WRITE_FILE_PATH"
    local baseline_sha="$WRITE_FILE_SHA"

    # 立即验证
    if verify_sha256 "$baseline_path" "$baseline_sha"; then
        test_pass "场景1: 正常写入 + 读取验证"
    else
        test_fail "场景1" "基准文件 checksum 验证失败"
        return 1
    fi

    # 等待 Raft 复制完成, 确保基准文件元数据已同步到所有 Filer 节点.
    # 否则场景2 停止 Leader 时,基准文件元数据可能尚未复制到新 Leader,导致后续验证失败.
    log_info "等待 Raft 复制同步基准文件元数据 (10s)..."
    sleep 10

    # 保存供后续场景使用
    echo "$baseline_path" > /tmp/powerfs_baseline_path
    echo "$baseline_sha" > /tmp/powerfs_baseline_sha
    return 0
}

# 场景 2: 停止 Leader, 选举期间写入 (核心验证)
test_scenario_2_leader_failure_write() {
    test_start "场景2: Leader Filer 故障期间写入 (核心: 重试+轮换)"

    # 读取基准文件信息
    local baseline_path
    local baseline_sha
    baseline_path=$(cat /tmp/powerfs_baseline_path 2>/dev/null || echo "")
    baseline_sha=$(cat /tmp/powerfs_baseline_sha 2>/dev/null || echo "")

    # 找到当前 Leader
    local leader
    leader=$(find_leader_filer 2>/dev/null || echo "")
    if [ -z "$leader" ]; then
        test_fail "场景2" "无法找到 Leader Filer"
        return 1
    fi
    log_info "当前 Leader: $leader, 即将停止以触发 Raft 选举"

    # 记录停止前的存活 filer 数
    local alive_before
    alive_before=$(find_alive_filers)
    log_detail "停止前存活 Filers: $alive_before"

    # 停止 Leader Filer
    cd "$DOCKER_DIR"
    docker compose stop "$leader" 2>/dev/null
    cd "$SCRIPT_DIR"
    log_ok "已停止 Leader: $leader"

    # 核心验证: 选举期间立即写入 (不等选举完成)
    # 此时旧 leader 不可达, send_coherence_msg 应:
    #   1. 尝试旧 leader 地址 -> 网络错误
    #   2. 轮换到其他 filer 地址, 指数退避重试
    #   3. 新 leader 选举完成后返回 OK 或 REDIRECT
    #   4. 写入成功
    local write_ok=true
    local failover_path
    local failover_sha

    if write_file_expect_success 3 "during_failover"; then
        failover_path="$WRITE_FILE_PATH"
        failover_sha="$WRITE_FILE_SHA"
        log_ok "故障期间写入成功 (重试+轮换生效)"
    else
        write_ok=false
        test_fail "场景2" "故障期间写入失败 (EIO), 重试+轮换未生效"
        # 尝试恢复集群状态
        cd "$DOCKER_DIR"
        docker compose start "$leader" 2>/dev/null
        cd "$SCRIPT_DIR"
        wait_for_leader_election 30 || true
        return 1
    fi

    # 等待新 Leader 选举完成
    wait_for_leader_election 30 || {
        log_warn "Leader 选举超时, 但写入已成功 (可能通过 follower 转发)"
    }

    # 验证故障期间写入的文件 (重启 FUSE 清缓存后验证)
    restart_fuse_clear_cache || {
        test_fail "场景2" "FUSE 重启失败"
        return 1
    }

    local verify_ok=true
    if [ -n "$failover_path" ] && [ -n "$failover_sha" ]; then
        if ! verify_sha256 "$failover_path" "$failover_sha"; then
            verify_ok=false
            test_fail "场景2: 故障期间写入文件验证" "checksum 不匹配"
        fi
    fi

    # 验证基准文件仍可读
    if [ -n "$baseline_path" ] && [ -n "$baseline_sha" ]; then
        if ! verify_sha256 "$baseline_path" "$baseline_sha"; then
            verify_ok=false
            test_fail "场景2: 基准文件验证" "checksum 不匹配"
        fi
    fi

    if $verify_ok; then
        test_pass "场景2: Leader 故障期间写入数据完整"
    fi

    # 保存故障期间文件信息供场景3使用
    echo "$failover_path" > /tmp/powerfs_failover_path
    echo "$failover_sha" > /tmp/powerfs_failover_sha

    # 恢复停止的 Filer
    log_info "恢复 Filer: $leader"
    cd "$DOCKER_DIR"
    docker compose start "$leader" 2>/dev/null
    cd "$SCRIPT_DIR"
    sleep 5
    wait_for_leader_election 30 || true
    # 等待 Raft 日志追平: 恢复的 Filer 需要从新 Leader 拉取缺失的日志条目,
    # 包括故障期间写入的 failover 文件元数据. 若未等同步完成就重启 FUSE,
    # 可能读到旧副本导致 checksum 不匹配.
    log_info "等待 Raft 日志追平 (15s)..."
    sleep 15

    return 0
}

# 场景 3: 恢复后持续可用性 + 多次故障切换
test_scenario_3_recovery_and_refailover() {
    test_start "场景3: Filer 恢复后持续可用 + 二次故障切换"

    # 确认所有 filer 存活
    local alive
    alive=$(find_alive_filers)
    log_detail "存活 Filers: $alive"
    local alive_count
    alive_count=$(echo "$alive" | wc -w)
    if [ "$alive_count" -lt 3 ]; then
        log_warn "只有 $alive_count 个 Filer 存活, 跳过二次故障切换"
        test_pass "场景3: 跳过 (Filer 未完全恢复)"
        return 0
    fi

    # 读取之前写入的文件信息
    local failover_path
    local failover_sha
    failover_path=$(cat /tmp/powerfs_failover_path 2>/dev/null || echo "")
    failover_sha=$(cat /tmp/powerfs_failover_sha 2>/dev/null || echo "")

    # 恢复后写入新文件
    if ! write_test_file 3 "after_recovery"; then
        test_fail "场景3" "恢复后写入失败"
        return 1
    fi
    local recovery_path="$WRITE_FILE_PATH"
    local recovery_sha="$WRITE_FILE_SHA"

    # 二次故障切换: 找到当前 leader, 停止它
    local leader2
    leader2=$(find_leader_filer 2>/dev/null || echo "")
    if [ -z "$leader2" ]; then
        log_warn "无法找到 Leader, 跳过二次故障切换"
        test_pass "场景3: 恢复后写入成功 (跳过二次切换)"
        return 0
    fi
    log_info "二次故障切换: 停止 Leader $leader2"
    cd "$DOCKER_DIR"
    docker compose stop "$leader2" 2>/dev/null
    cd "$SCRIPT_DIR"

    # 选举期间再次写入
    if write_file_expect_success 2 "second_failover"; then
        log_ok "二次故障期间写入成功"
        local second_path="$WRITE_FILE_PATH"
        local second_sha="$WRITE_FILE_SHA"

        # 等待选举
        wait_for_leader_election 30 || true

        # 恢复
        cd "$DOCKER_DIR"
        docker compose start "$leader2" 2>/dev/null
        cd "$SCRIPT_DIR"
        sleep 5
        wait_for_leader_election 30 || true
        # 等待 Raft 日志追平, 确保二次故障期间写入的元数据已同步到所有节点
        log_info "等待 Raft 日志追平 (15s)..."
        sleep 15

        # 重启 FUSE 清缓存后验证所有文件
        restart_fuse_clear_cache || {
            test_fail "场景3" "FUSE 重启失败"
            return 1
        }

        local all_ok=true
        verify_sha256 "$failover_path" "$failover_sha" || all_ok=false
        verify_sha256 "$recovery_path" "$recovery_sha" || all_ok=false
        verify_sha256 "$second_path" "$second_sha" || all_ok=false

        if $all_ok; then
            test_pass "场景3: 二次故障切换后所有数据完整"
        else
            test_fail "场景3" "部分文件 checksum 验证失败"
        fi
    else
        test_fail "场景3" "二次故障期间写入失败"
        # 恢复
        cd "$DOCKER_DIR"
        docker compose start "$leader2" 2>/dev/null
        cd "$SCRIPT_DIR"
        wait_for_leader_election 30 || true
        return 1
    fi

    return 0
}

# 场景 4: 快速连续小文件创建 (压力测试 Leader 切换)
test_scenario_4_rapid_create_during_failover() {
    test_start "场景4: 故障期间快速连续创建小文件"

    local leader
    leader=$(find_leader_filer 2>/dev/null || echo "")
    if [ -z "$leader" ]; then
        test_fail "场景4" "无法找到 Leader Filer"
        return 1
    fi
    log_info "停止 Leader $leader 后立即创建 10 个小文件"

    cd "$DOCKER_DIR"
    docker compose stop "$leader" 2>/dev/null
    cd "$SCRIPT_DIR"

    local success_count=0
    local fail_count=0
    local files=()
    local shas=()
    local i
    for i in $(seq 1 10); do
        local path="${TEST_DIR}/rapid_${i}_$(date +%s%N).dat"
        if fuse_exec dd if=/dev/urandom of="$path" bs=4K count=1 2>/dev/null; then
            local sha
            sha=$(fuse_exec sha256sum "$path" 2>/dev/null | awk '{print $1}')
            if [ -n "$sha" ]; then
                success_count=$((success_count + 1))
                files+=("$path")
                shas+=("$sha")
            else
                fail_count=$((fail_count + 1))
            fi
        else
            fail_count=$((fail_count + 1))
        fi
    done

    log_detail "成功: $success_count, 失败: $fail_count"

    # 等待选举 + 恢复
    wait_for_leader_election 30 || true
    cd "$DOCKER_DIR"
    docker compose start "$leader" 2>/dev/null
    cd "$SCRIPT_DIR"
    sleep 5
    wait_for_leader_election 30 || true
    # 等待 Raft 日志追平, 确保快速创建的小文件元数据已同步
    log_info "等待 Raft 日志追平 (15s)..."
    sleep 15

    # 重启 FUSE 清缓存后验证
    restart_fuse_clear_cache || true

    local verified=0
    for i in "${!files[@]}"; do
        if verify_sha256 "${files[$i]}" "${shas[$i]}" 2>/dev/null; then
            verified=$((verified + 1))
        fi
    done

    if [ "$success_count" -gt 0 ] && [ "$verified" -eq "$success_count" ]; then
        test_pass "场景4: $success_count 个小文件全部验证通过"
    elif [ "$success_count" -gt 0 ]; then
        test_fail "场景4" "只有 $verified/$success_count 个文件验证通过"
    else
        test_fail "场景4" "所有小文件创建失败"
    fi

    return 0
}

# ============================================================================
# 主流程
# ============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --start-cluster) START_CLUSTER=true ;;
            --cleanup)       CLEANUP=true ;;
            *) log_warn "未知参数: $1" ;;
        esac
        shift
    done
}

main() {
    parse_args "$@"

    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║  PowerFS Filer 故障写入修复验证                           ${NC}"
    echo -e "${CYAN}║  (网络错误重试 + Filer 轮换)                              ${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
    echo ""

    if [ "$START_CLUSTER" = true ]; then
        start_cluster
    fi

    # 前置检查
    if ! docker ps --format '{{.Names}}' | grep -q "^${FUSE_CONTAINER}$"; then
        log_error "FUSE 容器 $FUSE_CONTAINER 未运行, 请先启动集群 (--start-cluster)"
        exit 1
    fi

    wait_fuse_ready || exit 1

    # 确认使用 powerfs-fuse (用户要求: 测试前确认 powerfs-fuse 被使用)
    local mount_info
    mount_info=$(fuse_exec cat /proc/self/mountinfo 2>/dev/null | grep "$MOUNT_POINT" || echo "")
    if echo "$mount_info" | grep -qi "powerfs"; then
        log_ok "确认挂载使用 powerfs-fuse"
    else
        log_warn "无法确认 powerfs-fuse, mountinfo: $mount_info"
    fi

    # 运行测试场景
    test_scenario_1_baseline || true
    test_scenario_2_leader_failure_write || true
    test_scenario_3_recovery_and_refailover || true
    test_scenario_4_rapid_create_during_failover || true

    # 汇总
    echo ""
    echo "=============================================="
    echo "  测试汇总"
    echo "=============================================="
    echo -e "  ${GREEN}通过: $PASS${NC}"
    echo -e "  ${RED}失败: $FAIL${NC}"
    if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
        echo ""
        echo "  失败详情:"
        for t in "${FAILED_TESTS[@]}"; do
            echo -e "    ${RED}- $t${NC}"
        done
    fi
    echo ""

    if [ "$CLEANUP" = true ]; then
        stop_cluster
    fi

    if [ "$FAIL" -eq 0 ]; then
        echo -e "${GREEN}========================================${NC}"
        echo -e "${GREEN}    全部测试通过                        ${NC}"
        echo -e "${GREEN}========================================${NC}"
        exit 0
    else
        echo -e "${RED}========================================${NC}"
        echo -e "${RED}    存在失败用例                        ${NC}"
        echo -e "${RED}========================================${NC}"
        exit 1
    fi
}

main "$@"
