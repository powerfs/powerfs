#!/bin/bash
# =============================================================================
# PowerFS 统一测试入口
#
# 调度 FUSE 轨道与 kernel 轨道, 复用 scripts/lib/common.sh 日志/计数函数.
# 各轨道自带环境准备 (--no-env 可跳过), 本脚本只做调度+汇总.
#
# 用法:
#   ./run_all.sh                              # 全量 (FUSE + kernel), 含环境准备
#   ./run_all.sh --track fuse                 # 仅 FUSE 轨道
#   ./run_all.sh --track kernel               # 仅 kernel 轨道 (QEMU VM)
#   ./run_all.sh --track all --no-env         # 跳过环境准备
#   ./run_all.sh --track fuse -s T1 -s T2     # 指定阶段 (透传给子轨道)
#   ./run_all.sh -c                           # 失败继续 (默认门禁: 失败即停)
#
# 退出码: 0=全通过, 1=有失败, 2=环境失败, 3=参数错误
# =============================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${PROJECT_ROOT}/scripts/lib/common.sh" 2>/dev/null || {
    echo "[FATAL] scripts/lib/common.sh not found"; exit 2
}

# 轨道脚本路径
FUSE_RUNNER="${SCRIPT_DIR}/fuse/run_fuse_full_test.sh"
KERNEL_RUNNER="${PROJECT_ROOT}/kernel/vm/run_all_tests.sh"

# 默认参数
TRACK="all"
NO_ENV=0
CONTINUE_ON_FAIL=0
SELECTED_STAGES=()
FUSE_CONTAINER="${FUSE_CONTAINER:-fuse-1}"

# 日志目录
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${SCRIPT_DIR}/output/run_${TIMESTAMP}"
SUMMARY_FILE="${RUN_DIR}/summary.log"
mkdir -p "${RUN_DIR}"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --track NAME       fuse | kernel | all (default: all)
  --no-env           Skip environment preparation (assume services + mount ready)
  -c, --continue     Continue on failure (default: stop on first failure, gate mode)
  -s STAGE           Run only specified stage (T1/T2/T3/T4/T5/T6/T7/T8, repeatable)
  --fuse-container N Container name for FUSE track (default: fuse-1)
  -h, --help         Show this help

Tracks:
  fuse    FUSE client tests in docker container (L1-L6 layered)
  kernel  Kernel FS tests in QEMU VM (T1-T9 + K1-K4)
  all     Both tracks (FUSE first, then kernel)
EOF
}

# 参数解析
while [ $# -gt 0 ]; do
    case "$1" in
        --track)         TRACK="$2"; shift 2 ;;
        --no-env)        NO_ENV=1; shift ;;
        -c|--continue)  CONTINUE_ON_FAIL=1; shift ;;
        -s)              SELECTED_STAGES+=("$2"); shift 2 ;;
        --fuse-container) FUSE_CONTAINER="$2"; shift 2 ;;
        -h|--help)       usage; exit 0 ;;
        *)               echo "Unknown option: $1"; usage; exit 3 ;;
    esac
done

# 校验 track 值
case "$TRACK" in
    fuse|kernel|all) ;;
    *) echo "Invalid --track: $TRACK"; usage; exit 3 ;;
esac

# 透传参数构造
COMMON_ARGS=()
[ $NO_ENV -eq 1 ] && COMMON_ARGS+=("--no-env")
[ $CONTINUE_ON_FAIL -eq 1 ] && COMMON_ARGS+=("-c")
for s in "${SELECTED_STAGES[@]+"${SELECTED_STAGES[@]}"}"; do
    COMMON_ARGS+=("-s" "$s")
done

log_info "=========================================="
log_info " PowerFS Unified Test Runner"
log_info " Track: $TRACK | Container: $FUSE_CONTAINER"
log_info " Output: $RUN_DIR"
log_info "=========================================="

GLOBAL_PASS=0
GLOBAL_FAIL=0
TRACK_RESULTS=()

# -----------------------------------------------------------------------------
# FUSE 轨道: 容器内执行 run_fuse_full_test.sh
# -----------------------------------------------------------------------------
run_fuse_track() {
    log_info "[FUSE] Starting FUSE track in container '$FUSE_CONTAINER'"

    if [ ! -f "$FUSE_RUNNER" ]; then
        log_error "[FUSE] Runner not found: $FUSE_RUNNER"
        return 2
    fi

    # 确认容器在运行
    if ! docker ps --format '{{.Names}}' | grep -q "^${FUSE_CONTAINER}$"; then
        log_error "[FUSE] Container '$FUSE_CONTAINER' not running"
        log_info  "[FUSE] Start cluster first: docker/scripts/start-cluster.sh"
        return 2
    fi

    # 确认 FUSE 已挂载
    if ! docker exec "$FUSE_CONTAINER" mount 2>/dev/null | grep -q "on /mnt/powerfs type fuse"; then
        log_error "[FUSE] /mnt/powerfs not mounted in '$FUSE_CONTAINER'"
        log_info  "[FUSE] Check fuse.toml mount_point and container restart"
        return 2
    fi

    # 拷贝脚本到容器
    local remote="/tmp/fuse_full_test.sh"
    log_info "[FUSE] Copying runner to container..."
    if ! docker cp "$FUSE_RUNNER" "${FUSE_CONTAINER}:${remote}" 2>&1 | tee -a "$SUMMARY_FILE"; then
        log_error "[FUSE] docker cp failed"
        return 2
    fi

    # 设置环境变量并执行
    local env_args=(-e SKIP_KERNEL_E2E=1)
    [ $NO_ENV -eq 1 ] || env_args+=()  # FUSE 轨道无独立 env prep, 由 docker cluster 已起

    log_info "[FUSE] Executing runner in container..."
    if docker exec "${env_args[@]}" "$FUSE_CONTAINER" bash "$remote" 2>&1 | tee -a "${RUN_DIR}/fuse_track.log"; then
        log_success "[FUSE] Track PASSED"
        TRACK_RESULTS+=("FUSE:PASSED")
        return 0
    else
        local rc=$?
        log_error "[FUSE] Track FAILED (rc=$rc)"
        TRACK_RESULTS+=("FUSE:FAILED")
        return 1
    fi
}

# -----------------------------------------------------------------------------
# kernel 轨道: 调用 kernel/vm/run_all_tests.sh (VM 内执行)
# -----------------------------------------------------------------------------
run_kernel_track() {
    log_info "[KERNEL] Starting kernel track in QEMU VM"

    if [ ! -f "$KERNEL_RUNNER" ]; then
        log_error "[KERNEL] Runner not found: $KERNEL_RUNNER"
        return 2
    fi

    # kernel 轨道自带 --no-env/-c/-s 参数解析
    log_info "[KERNEL] Executing: $KERNEL_RUNNER ${COMMON_ARGS[*]}"
    if bash "$KERNEL_RUNNER" "${COMMON_ARGS[@]}" 2>&1 | tee -a "${RUN_DIR}/kernel_track.log"; then
        log_success "[KERNEL] Track PASSED"
        TRACK_RESULTS+=("KERNEL:PASSED")
        return 0
    else
        local rc=$?
        log_error "[KERNEL] Track FAILED (rc=$rc)"
        TRACK_RESULTS+=("KERNEL:FAILED")
        return 1
    fi
}

# -----------------------------------------------------------------------------
# 主流程
# -----------------------------------------------------------------------------
START_EPOCH=$(date +%s)
OVERALL_RC=0

if [ "$TRACK" = "fuse" ] || [ "$TRACK" = "all" ]; then
    if ! run_fuse_track; then
        OVERALL_RC=1
        if [ $CONTINUE_ON_FAIL -eq 0 ] && [ "$TRACK" = "all" ]; then
            log_warn "[GATE] FUSE track failed, stopping (use -c to continue to kernel)"
            break
        fi
    fi
fi

if [ "$TRACK" = "kernel" ] || [ "$TRACK" = "all" ]; then
    if [ $OVERALL_RC -ne 0 ] && [ $CONTINUE_ON_FAIL -eq 0 ]; then
        log_warn "[GATE] Skipping kernel track due to FUSE failure"
    else
        if ! run_kernel_track; then
            OVERALL_RC=1
        fi
    fi
fi

END_EPOCH=$(date +%s)
DURATION=$((END_EPOCH - START_EPOCH))

# 汇总
echo "" | tee -a "$SUMMARY_FILE"
log_info "==========================================" | tee -a "$SUMMARY_FILE"
log_info " Test Summary (duration: ${DURATION}s)"    | tee -a "$SUMMARY_FILE"
log_info "==========================================" | tee -a "$SUMMARY_FILE"
for r in "${TRACK_RESULTS[@]+"${TRACK_RESULTS[@]}"}"; do
    echo "  - $r" | tee -a "$SUMMARY_FILE"
done
echo "" | tee -a "$SUMMARY_FILE"
echo "Full logs: $RUN_DIR" | tee -a "$SUMMARY_FILE"

exit $OVERALL_RC
