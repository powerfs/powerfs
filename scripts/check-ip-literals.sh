#!/usr/bin/env bash
# P4 防回归: 禁止新增硬编码私网 IP 字面量.
#
# 单源原则: 集群地址只应来自 cluster.toml / docker/topology.env / 渲染产物,
# 不应散落在脚本、Rust 源码或 CI 配置里. 历史存量已收录到基线文件
# scripts/ip-literal-baseline.txt (path<TAB>出现次数), 本脚本保证:
#   - 基线外的文件出现私网 IP        -> 失败 (新增文件)
#   - 基线内文件出现次数超过基线      -> 失败 (新增字面量)
#   - 出现次数减少/文件删除           -> 通过 (提示可刷新基线)
#
# 基线只覆盖父仓库 git 索引中的文本文件; docs/ 与 *.md 允许示例地址, 不在范围.
# kernel/ 是 submodule, 文件不在父仓库索引内, 需要时在 kernel 仓库单独接入.
#
# 用法:
#   scripts/check-ip-literals.sh            # 检查 (CI 使用)
#   scripts/check-ip-literals.sh --update   # 清理存量后刷新基线
#   scripts/check-ip-literals.sh --verbose  # 额外打印所有基线条目

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BASELINE="${SCRIPT_DIR}/ip-literal-baseline.txt"

# RFC1918 私网段: 10/8, 172.16/12, 192.168/16.
# 前后用非数字边界避免匹配到更长的数字串.
IP_PATTERN='(^|[^0-9])(10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}|192\.168\.[0-9]{1,3}\.[0-9]{1,3})([^0-9]|$)'

MODE="check"
VERBOSE=0
case "${1:-}" in
    --update)  MODE="update" ;;
    --verbose) VERBOSE=1 ;;
    --help|-h)
        sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        exit 0 ;;
    "") ;;
    *) echo "Unknown option: $1 (try --help)" >&2; exit 2 ;;
esac

# 输出 "path:count", 每条路径一行; 无匹配时 git grep 退出码为 1.
collect() {
    git -C "${ROOT}" grep -c -I -o -E "${IP_PATTERN}" -- . ':!docs/**' ':!*.md' 2>/dev/null || true
}

if [ "${MODE}" = "update" ]; then
    {
        echo "# 硬编码私网 IP 基线 (P4 防回归)"
        echo "# 格式: <path><TAB><出现次数>; 由 scripts/check-ip-literals.sh --update 生成."
        echo "# 新增行表示新增存量: 清理后请重新 --update, 不要把新字面量加进来."
        # collect 输出 "path:count"; 基线用 TAB 分隔, 避免与路径中的冒号混淆
        collect | sed 's/:\([0-9]\+\)$/\t\1/' | LC_ALL=C sort
    } > "${BASELINE}"
    echo "Baseline updated: ${BASELINE} ($(grep -c -v '^#' "${BASELINE}" || true) files)"
    exit 0
fi

if [ ! -f "${BASELINE}" ]; then
    echo "::error::baseline missing: ${BASELINE} (run scripts/check-ip-literals.sh --update)" >&2
    exit 2
fi

declare -A ALLOWED=()
while IFS=$'\t' read -r path count; do
    case "${path}" in ""|\#*) continue ;; esac
    ALLOWED["${path}"]="${count}"
done < "${BASELINE}"

declare -A CURRENT=()
while IFS=: read -r path count; do
    [ -n "${path}" ] || continue
    CURRENT["${path}"]="${count}"
done < <(collect)

if [ "${VERBOSE}" = "1" ]; then
    echo "Baseline entries:"
    for path in $(printf '%s\n' "${!ALLOWED[@]}" | LC_ALL=C sort); do
        printf '  %-60s %s (current %s)\n' "${path}" "${ALLOWED[${path}]}" "${CURRENT[${path}]:-0}"
    done
fi

new_files=()
increased=()
total_files=0
total_hits=0
for path in $(printf '%s\n' "${!CURRENT[@]}" | LC_ALL=C sort); do
    count="${CURRENT[${path}]}"
    total_files=$((total_files + 1))
    total_hits=$((total_hits + count))
    if [ -z "${ALLOWED[${path}]+x}" ]; then
        new_files+=("${path}:${count}")
    elif [ "${count}" -gt "${ALLOWED[${path}]}" ]; then
        increased+=("${path}: ${ALLOWED[${path}]} -> ${count}")
    fi
done

failed=0
for item in "${new_files[@]:-}"; do
    [ -n "${item}" ] || continue
    path="${item%%:*}"
    echo "::error file=${path}::new file with hardcoded private IP literal"
    echo "NEW       ${item}"
    failed=1
done
for item in "${increased[@]:-}"; do
    [ -n "${item}" ] || continue
    path="${item%%:*}"
    echo "::error file=${path}::hardcoded private IP literals increased"
    echo "INCREASED ${item}"
    failed=1
done

if [ "${failed}" = "1" ]; then
    echo ""
    echo "Hardcoded private IP literals must come from cluster.toml / topology.env / rendered config."
    echo "If this is a deliberate test fixture, keep it in a baseline-exempt location (docs/, *.md) and regenerate:"
    echo "  scripts/check-ip-literals.sh --update"
    exit 1
fi

stale=$(comm -23 \
    <(grep -v '^#' "${BASELINE}" | cut -f1 | LC_ALL=C sort) \
    <(printf '%s\n' "${!CURRENT[@]}" | LC_ALL=C sort))
if [ -n "${stale}" ]; then
    echo "NOTE: ${stale//$'\n'/, } no longer contain private IP literals — baseline can be refreshed with --update."
fi

echo "IP literal lint OK: ${total_files} files, ${total_hits} occurrences within baseline ($(grep -c -v '^#' "${BASELINE}" || true) allowed files)"
