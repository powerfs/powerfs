#!/usr/bin/env bash
# ================================================================
# PowerFS 功能测试 — 全量运行入口
#
# 按 T1→T2→T3→T4→T8 顺序执行，同阶段 FAIL 不阻断后续（汇总报告）。
# 每个脚本独立运行，使用唯一时间戳目录隔离。
#
# 用法:
#   bash tests/functional/run_all.sh              # 运行全部
#   bash tests/functional/run_all.sh t1 t2        # 仅运行 T1 和 T2
#   bash tests/functional/run_all.sh --stop-on-fail  # 首次失败即停止
# ================================================================
set -u
cd "$(dirname "$0")/../.."

STAGE=""
STOP_ON_FAIL=false
TOTAL_PASS=0
TOTAL_FAIL=0
STAGES_RUN=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --stop-on-fail) STOP_ON_FAIL=true; shift ;;
        t1|T1) STAGES_RUN+=("t1"); shift ;;
        t2|T2) STAGES_RUN+=("t2"); shift ;;
        t3|T3) STAGES_RUN+=("t3"); shift ;;
        t4|T4) STAGES_RUN+=("t4"); shift ;;
        t8|T8) STAGES_RUN+=("t8"); shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# Default: run all
if [[ ${#STAGES_RUN[@]} -eq 0 ]]; then
    STAGES_RUN=("t1" "t2" "t3" "t4" "t8")
fi

echo ""
echo "================================================================"
echo "  PowerFS Functional Test Suite"
echo "  Stages: ${STAGES_RUN[*]}"
echo "  Stop on fail: $STOP_ON_FAIL"
echo "  Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo "================================================================"

for stage in "${STAGES_RUN[@]}"; do
    script="tests/functional/${stage}_*.sh"
    script_path=$(ls $script 2>/dev/null | head -1)

    if [[ -z "$script_path" ]]; then
        echo ""
        echo "  [WARN] No script found for stage $stage"
        continue
    fi

    echo ""
    echo "================================================================"
    echo "  Running: $script_path"
    echo "================================================================"

    bash "$script_path"
    exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        echo ""
        echo "  >>> Stage $stage FAILED (exit code $exit_code)"
        TOTAL_FAIL=$((TOTAL_FAIL + 1))
        if $STOP_ON_FAIL; then
            echo "  >>> --stop-on-fail: stopping"
            break
        fi
    else
        echo ""
        echo "  >>> Stage $stage PASSED"
        TOTAL_PASS=$((TOTAL_PASS + 1))
    fi
done

echo ""
echo "================================================================"
echo "  Final Summary"
echo "================================================================"
echo "  Stages passed: $TOTAL_PASS"
echo "  Stages failed: $TOTAL_FAIL"

if [[ $TOTAL_FAIL -gt 0 ]]; then
    echo "  RESULT: FAILED"
    exit 1
else
    echo "  RESULT: ALL PASS"
    exit 0
fi
