#!/bin/bash
#
# PowerFS 依赖安装脚本
# 适用于 Ubuntu 22.04+/Debian 11+ 新环境
#
# Usage:
#   sudo bash deps.sh              # 安装所有依赖（不含 SPDK）
#   sudo bash deps.sh --spdk       # 安装所有依赖 + SPDK 编译环境（不自动编译SPDK）
#   sudo bash deps.sh --docker     # 仅安装 Docker
#   sudo bash deps.sh --rust       # 仅安装 Rust
#   sudo bash deps.sh --node       # 仅安装 Node.js + pnpm
#   sudo bash deps.sh --python     # 仅安装 Python

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        error "请使用 sudo 运行此脚本"
    fi
}

install_system_deps() {
    info "=== 安装系统基础依赖 ==="
    apt-get update -y
    apt-get install -y \
        build-essential \
        cmake \
        pkg-config \
        libssl-dev \
        libz-dev \
        libclang-dev \
        clang \
        llvm \
        git \
        curl \
        wget \
        unzip \
        tar \
        libnuma-dev \
        libaio-dev \
        libglib2.0-dev \
        libfuse3-dev \
        fuse3 \
        uuid-dev \
        liburing-dev \
        liblz4-dev \
        zlib1g-dev \
        libsnappy-dev \
        libprotobuf-dev \
        protobuf-compiler \
        libgrpc-dev \
        libgrpc++-dev \
        grpc-compiler \
        libsqlite3-dev \
        libjemalloc-dev
}

install_rust() {
    info "=== 安装 Rust (rustup) ==="
    if command -v rustc &> /dev/null; then
        info "Rust 已安装: $(rustc --version)"
        return
    fi
    
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
    
    info "Rust 安装完成: $(rustc --version)"
    info "Cargo 版本: $(cargo --version)"
}

install_node() {
    info "=== 安装 Node.js 22 LTS + pnpm ==="
    if command -v node &> /dev/null; then
        info "Node.js 已安装: $(node --version)"
    else
        curl -fsSL https://nodejs.org/dist/v22.13.0/node-v22.13.0-linux-x64.tar.xz | tar -xJ -C /usr/local --strip-components=1
        info "Node.js 安装完成: $(node --version)"
    fi
    
    if command -v pnpm &> /dev/null; then
        info "pnpm 已安装: $(pnpm --version)"
    else
        curl -fsSL https://get.pnpm.io/install.sh | sh -
        info "pnpm 安装完成"
    fi
}

install_docker() {
    info "=== 安装 Docker ==="
    if command -v docker &> /dev/null; then
        info "Docker 已安装: $(docker --version)"
    else
        apt-get install -y \
            ca-certificates \
            curl \
            gnupg \
            lsb-release
        
        mkdir -p /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        
        echo \
            "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
            $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
        
        apt-get update -y
        apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
        
        info "Docker 安装完成: $(docker --version)"
        info "Docker Compose 安装完成: $(docker compose version)"
        
        usermod -aG docker "$SUDO_USER"
        info "已将用户 $SUDO_USER 添加到 docker 组"
    fi
}

install_python() {
    info "=== 安装 Python 3 ==="
    apt-get install -y \
        python3 \
        python3-dev \
        python3-pip \
        python3-venv
    
    info "Python 安装完成: $(python3 --version)"
    info "pip 安装完成: $(pip3 --version)"
    
    pip3 install --upgrade pip
    
    info "安装 Python 开发依赖..."
    pip3 install \
        numpy \
        pandas \
        matplotlib \
        requests \
        grpcio \
        grpcio-tools \
        protobuf \
        pytest \
        pytest-asyncio \
        httpx \
        pyyaml
    
    info "Python 依赖安装完成"
}

install_spdk_deps() {
    info "=== 安装 SPDK 编译依赖 ==="
    apt-get install -y \
        libnuma-dev \
        libaio-dev \
        libssl-dev \
        libglib2.0-dev \
        libfuse3-dev \
        uuid-dev \
        liburing-dev \
        liblz4-dev \
        zlib1g-dev \
        libsnappy-dev \
        libprotobuf-dev \
        protobuf-compiler \
        libgrpc-dev \
        libgrpc++-dev \
        grpc-compiler \
        autoconf \
        automake \
        libtool \
        flex \
        bison \
        libxml2-dev \
        libjson-c-dev \
        libboost-all-dev
    
    info "SPDK 编译依赖安装完成"
    
    warn "SPDK 需要源码编译，以下是编译步骤（手动执行）："
    warn ""
    warn "1. 获取 SPDK 源码："
    warn "   git clone https://github.com/spdk/spdk.git"
    warn "   cd spdk && git checkout v24.05"
    warn ""
    warn "2. 安装 SPDK 依赖脚本："
    warn "   ./scripts/pkgdep.sh"
    warn ""
    warn "3. 编译 SPDK："
    warn "   ./configure --enable-debug"
    warn "   make -j$(nproc)"
    warn ""
    warn "4. 安装 SPDK（可选）："
    warn "   make install"
    warn ""
    warn "5. 设置环境变量："
    warn "   export SPDK_ROOT_DIR=/path/to/spdk"
    warn "   export PATH=\$SPDK_ROOT_DIR/bin:\$PATH"
    warn ""
    warn "6. 配置 hugepages（必须）："
    warn "   sudo sysctl -w vm.nr_hugepages=1024"
    warn "   sudo mkdir -p /mnt/huge"
    warn "   sudo mount -t hugetlbfs nodev /mnt/huge"
    warn ""
    warn "7. 绑定 NVMe 设备到 VFIO（必须）："
    warn "   sudo modprobe vfio-pci"
    warn "   sudo \$SPDK_ROOT_DIR/scripts/setup.sh"
    warn ""
    warn "注意：SPDK 编译耗时较长（约 10-30 分钟），建议在单独的终端执行"
}

show_help() {
    echo "PowerFS 依赖安装脚本"
    echo ""
    echo "Usage:"
    echo "  sudo bash deps.sh              # 安装所有依赖（不含 SPDK）"
    echo "  sudo bash deps.sh --spdk       # 安装所有依赖 + SPDK 编译环境"
    echo "  sudo bash deps.sh --docker     # 仅安装 Docker"
    echo "  sudo bash deps.sh --rust       # 仅安装 Rust"
    echo "  sudo bash deps.sh --node       # 仅安装 Node.js + pnpm"
    echo "  sudo bash deps.sh --python     # 仅安装 Python"
    echo "  sudo bash deps.sh --all        # 安装所有依赖（含 SPDK 编译环境）"
    echo "  sudo bash deps.sh --help       # 显示帮助"
    echo ""
}

main() {
    check_root
    
    INSTALL_ALL=true
    INSTALL_SPDK=false
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --spdk|--all)
                INSTALL_SPDK=true
                shift
                ;;
            --docker)
                INSTALL_ALL=false
                install_docker
                exit 0
                ;;
            --rust)
                INSTALL_ALL=false
                install_rust
                exit 0
                ;;
            --node)
                INSTALL_ALL=false
                install_node
                exit 0
                ;;
            --python)
                INSTALL_ALL=false
                install_python
                exit 0
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                shift
                ;;
        esac
    done
    
    if $INSTALL_ALL; then
        info "========================================"
        info "  PowerFS 环境依赖安装"
        info "========================================"
        
        install_system_deps
        install_rust
        install_node
        install_docker
        install_python
        
        if $INSTALL_SPDK; then
            install_spdk_deps
        fi
        
        info ""
        info "========================================"
        info "  依赖安装完成！"
        info "========================================"
        info ""
        info "环境验证命令："
        info "  rustc --version    # 验证 Rust"
        info "  cargo --version    # 验证 Cargo"
        info "  node --version     # 验证 Node.js"
        info "  pnpm --version     # 验证 pnpm"
        info "  docker --version   # 验证 Docker"
        info "  python3 --version  # 验证 Python"
        info ""
        if ! $INSTALL_SPDK; then
            warn "如需使用 SPDK 后端，请运行: sudo bash deps.sh --spdk"
            warn "然后按照提示手动编译 SPDK"
        fi
    fi
}

main "$@"