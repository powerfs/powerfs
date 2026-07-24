#!/bin/bash
#
# prep.sh — PowerFS 构建准备脚本
#
# 从 Cargo.toml.community 生成 powerfs-fuse/Cargo.toml
#

set -euo pipefail

SCRIPT_CALLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_CALLER_DIR}/.." && pwd)"

FUSE_CARGO_COMMUNITY="${ROOT_DIR}/powerfs-fuse/Cargo.toml.community"
FUSE_CARGO="${ROOT_DIR}/powerfs-fuse/Cargo.toml"

main() {
    echo "==> 配置社区版 (community)"

    if [ ! -f "${FUSE_CARGO_COMMUNITY}" ]; then
        echo "错误：缺少模板文件 ${FUSE_CARGO_COMMUNITY}" >&2
        exit 1
    fi

    cp "${FUSE_CARGO_COMMUNITY}" "${FUSE_CARGO}"
    echo "    生成 powerfs-fuse/Cargo.toml (来自 Cargo.toml.community)"

    echo ""
    echo "==> 配置完成，现在可以运行：cargo build"
}

main
