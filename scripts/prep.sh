#!/bin/bash
#
# prep.sh — PowerFS 构建准备脚本
#
# 根据当前工作区是否包含 powerfs-fuse-enterprise 目录，
# 自动切换社区版/企业版配置：
#   1. 生成 powerfs-fuse/Cargo.toml（来自 .community 或 .enterprise 模板）
#   2. 调整根 Cargo.toml 的 workspace members
#      （企业版包含 powerfs-fuse-enterprise，社区版不包含）
#
# 用法：
#   ./prep.sh              # 自动检测版本并配置
#   ./prep.sh community    # 强制按社区版配置
#   ./prep.sh enterprise   # 强制按企业版配置

set -euo pipefail

SCRIPT_CALLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_CALLER_DIR}/.." && pwd)"

WORKSPACE_CARGO="${ROOT_DIR}/Cargo.toml"
FUSE_CARGO_COMMUNITY="${ROOT_DIR}/powerfs-fuse/Cargo.toml.community"
FUSE_CARGO_ENTERPRISE="${ROOT_DIR}/powerfs-fuse/Cargo.toml.enterprise"
FUSE_CARGO="${ROOT_DIR}/powerfs-fuse/Cargo.toml"
ENTERPRISE_DIR="${ROOT_DIR}/powerfs-fuse-enterprise"

# workspace members 中企业版成员所在行（保持与 Cargo.toml 缩进一致）
ENTERPRISE_MEMBER_LINE='    "powerfs-fuse-enterprise",'
FUSE_CORE_ANCHOR='    "powerfs-fuse-core",'

# 自动检测版本：根据 powerfs-fuse-enterprise 目录是否存在
detect_edition() {
    if [ -d "${ENTERPRISE_DIR}" ] && [ -f "${ENTERPRISE_DIR}/Cargo.toml" ]; then
        echo "enterprise"
    else
        echo "community"
    fi
}

configure_community() {
    echo "==> 配置社区版 (community)"

    # 1. 从 .community 模板生成 powerfs-fuse/Cargo.toml
    if [ ! -f "${FUSE_CARGO_COMMUNITY}" ]; then
        echo "错误：缺少模板文件 ${FUSE_CARGO_COMMUNITY}" >&2
        exit 1
    fi
    cp "${FUSE_CARGO_COMMUNITY}" "${FUSE_CARGO}"
    echo "    生成 powerfs-fuse/Cargo.toml (来自 Cargo.toml.community)"

    # 2. 从 workspace members 移除 powerfs-fuse-enterprise
    if grep -qF "${ENTERPRISE_MEMBER_LINE}" "${WORKSPACE_CARGO}"; then
        sed -i "\|${ENTERPRISE_MEMBER_LINE}|d" "${WORKSPACE_CARGO}"
        echo "    从 workspace members 移除 powerfs-fuse-enterprise"
    else
        echo "    workspace members 已不包含 powerfs-fuse-enterprise (无需修改)"
    fi
}

configure_enterprise() {
    echo "==> 配置企业版 (enterprise)"

    if [ ! -d "${ENTERPRISE_DIR}" ] || [ ! -f "${ENTERPRISE_DIR}/Cargo.toml" ]; then
        echo "错误：未找到 powerfs-fuse-enterprise 目录或其 Cargo.toml" >&2
        echo "请确认企业版代码已检出（例如：git submodule update --init）" >&2
        exit 1
    fi

    # 1. 从 .enterprise 模板生成 powerfs-fuse/Cargo.toml
    if [ ! -f "${FUSE_CARGO_ENTERPRISE}" ]; then
        echo "错误：缺少模板文件 ${FUSE_CARGO_ENTERPRISE}" >&2
        exit 1
    fi
    cp "${FUSE_CARGO_ENTERPRISE}" "${FUSE_CARGO}"
    echo "    生成 powerfs-fuse/Cargo.toml (来自 Cargo.toml.enterprise)"

    # 2. 向 workspace members 添加 powerfs-fuse-enterprise（幂等）
    if grep -qF "${ENTERPRISE_MEMBER_LINE}" "${WORKSPACE_CARGO}"; then
        echo "    workspace members 已包含 powerfs-fuse-enterprise (无需修改)"
    else
        # 插入到 "powerfs-fuse-core", 之后，保持与原顺序一致
        sed -i "s|${FUSE_CORE_ANCHOR}|${FUSE_CORE_ANCHOR}\n${ENTERPRISE_MEMBER_LINE}|" "${WORKSPACE_CARGO}"
        echo "    向 workspace members 添加 powerfs-fuse-enterprise"
    fi
}

main() {
    local edition

    if [ $# -ge 1 ]; then
        case "$1" in
            community)
                edition="community"
                ;;
            enterprise)
                edition="enterprise"
                ;;
            *)
                echo "用法：$0 [community|enterprise]" >&2
                exit 2
                ;;
        esac
    else
        edition=$(detect_edition)
    fi

    echo "PowerFS 构建准备"
    echo "  工作目录: ${ROOT_DIR}"
    echo "  检测版本: $(detect_edition)"
    echo "  目标版本: ${edition}"
    echo ""

    case "${edition}" in
        community)
            configure_community
            ;;
        enterprise)
            configure_enterprise
            ;;
    esac

    echo ""
    echo "==> 配置完成，现在可以运行：cargo build"
}

main "$@"
