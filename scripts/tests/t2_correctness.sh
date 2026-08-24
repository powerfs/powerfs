#!/bin/bash
# PowerFS T2 文件系统正确性测试 (FUSE 客户端)
# 测试: T2.1 大目录树 cp -r / T2.2 tar czf+xzf / T2.3 源码编译 / T2.4 rsync -a / T2.5 git
# 用法: docker exec fuse-1 bash /tests/t2_correctness.sh
# 或:   bash t2_correctness.sh (在容器内直接运行)

set -euo pipefail

MNT="/mnt/powerfs"
PASS=0
FAIL=0
SKIP=0

ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP] $1"; SKIP=$((SKIP+1)); }

TESTDIR="${MNT}/t2_test_$$"
WORKDIR="/tmp/t2_work_$$"
SRC_DIR="${WORKDIR}/src"
DST_DIR="${TESTDIR}/dst"

rm -rf "${TESTDIR}" 2>/dev/null || true
rm -rf "${WORKDIR}" 2>/dev/null || true
mkdir -p "${TESTDIR}" "${WORKDIR}" "${SRC_DIR}"

echo "=========================================="
echo "PowerFS T2 文件系统正确性测试"
echo "挂载点: ${MNT}"
echo "测试目录: ${TESTDIR}"
echo "工作目录: ${WORKDIR}"
echo "时间: $(date)"
echo "=========================================="

# ============================================================
echo ""
echo "--- T2.1 大目录树 cp -r (1000+ 文件) ---"

# 在本地 /tmp 生成源目录树 (1000+ 文件，混合大小)
echo "  生成源目录树 ${SRC_DIR} (1000+ 文件)..."
mkdir -p "${SRC_DIR}/deep/nested/tree"
for i in $(seq 1 200); do
    echo "small file ${i} content" > "${SRC_DIR}/small_${i}.txt"
done
for i in $(seq 1 50); do
    dd if=/dev/urandom of="${SRC_DIR}/mid_${i}.dat" bs=4K count=4 2>/dev/null
done
for i in $(seq 1 20); do
    dd if=/dev/urandom of="${SRC_DIR}/big_${i}.dat" bs=1M count=1 2>/dev/null
done
for i in $(seq 1 800); do
    echo "x" > "${SRC_DIR}/deep/nested/tree/f_${i}"
done
# 计数
SRC_COUNT=$(find "${SRC_DIR}" -type f | wc -l)
echo "  源文件数: ${SRC_COUNT}"

# cp -r 到 PowerFS
T1_CP_START=$(date +%s%N)
if cp -r "${SRC_DIR}" "${DST_DIR}"; then
    T1_CP_END=$(date +%s%N)
    T1_CP_MS=$(( (T1_CP_END - T1_CP_START) / 1000000 ))
    ok "cp -r 完成 (${T1_CP_MS} ms)"
else
    T1_CP_END=$(date +%s%N)
    T1_CP_MS=$(( (T1_CP_END - T1_CP_START) / 1000000 ))
    fail "cp -r 失败 (${T1_CP_MS} ms)"
fi

# 目标计数
DST_COUNT=$(find "${DST_DIR}" -type f | wc -l)
if [ "${DST_COUNT}" -eq "${SRC_COUNT}" ]; then
    ok "目标文件数匹配 (${DST_COUNT})"
else
    fail "目标文件数不匹配 (源 ${SRC_COUNT} / 目标 ${DST_COUNT})"
fi

# diff -r 完全一致
if diff -r "${SRC_DIR}" "${DST_DIR}" > /dev/null 2>&1; then
    ok "diff -r 源/目标完全一致"
else
    fail "diff -r 发现差异"
    # 输出前 10 行差异
    diff -r "${SRC_DIR}" "${DST_DIR}" 2>&1 | head -10 | sed 's/^/    /'
fi

# 二次读取 MD5 一致性 (从 PowerFS 读回 vs 源)
MANIFEST_SRC="${WORKDIR}/manifest_src.txt"
MANIFEST_DST="${WORKDIR}/manifest_dst.txt"
( cd "${SRC_DIR}" && find . -type f -exec md5sum {} \; | sort > "${MANIFEST_SRC}" ) 2>/dev/null
( cd "${DST_DIR}" && find . -type f -exec md5sum {} \; | sort > "${MANIFEST_DST}" ) 2>/dev/null
if diff "${MANIFEST_SRC}" "${MANIFEST_DST}" > /dev/null 2>&1; then
    ok "MD5 manifest 完全一致"
else
    fail "MD5 manifest 不一致"
    diff "${MANIFEST_SRC}" "${MANIFEST_DST}" 2>&1 | head -10 | sed 's/^/    /'
fi

# ============================================================
echo ""
echo "--- T2.2 tar czf + tar xzf ---"

# 将 DST_DIR 打包 (从 PowerFS 读)
TAR_FILE="${WORKDIR}/test.tar.gz"
T2_TAR_START=$(date +%s%N)
if tar czf "${TAR_FILE}" -C "${DST_DIR}" . 2>/dev/null; then
    T2_TAR_END=$(date +%s%N)
    T2_TAR_MS=$(( (T2_TAR_END - T2_TAR_START) / 1000000 ))
    ok "tar czf 完成 (${T2_TAR_MS} ms)"
else
    T2_TAR_END=$(date +%s%N)
    T2_TAR_MS=$(( (T2_TAR_END - T2_TAR_START) / 1000000 ))
    fail "tar czf 失败 (${T2_TAR_MS} ms)"
fi

# 解包到 PowerFS
TAR_DST="${TESTDIR}/tar_extract"
mkdir -p "${TAR_DST}"
if tar xzf "${TAR_FILE}" -C "${TAR_DST}" 2>/dev/null; then
    ok "tar xzf 完成"
else
    fail "tar xzf 失败"
fi

# 对比 tar 解包内容 vs DST_DIR
if diff -r "${DST_DIR}" "${TAR_DST}" > /dev/null 2>&1; then
    ok "tar 解包内容与源一致"
else
    fail "tar 解包内容与源不一致"
fi

# ============================================================
echo ""
echo "--- T2.3 源码编译 (tar -xf linux-src && make defconfig) ---"

# 检查是否提供 LINUX_SRC 环境变量 (linux 源码 tar 包)
if [ -n "${LINUX_SRC:-}" ] && [ -f "${LINUX_SRC}" ]; then
    LINUX_EXTRACT="${TESTDIR}/linux_src"
    mkdir -p "${LINUX_EXTRACT}"
    if tar -xf "${LINUX_SRC}" -C "${LINUX_EXTRACT}" 2>/dev/null; then
        ok "linux 源码 tar -xf 到 PowerFS 成功"
        LINUX_DIR=$(find "${LINUX_EXTRACT}" -maxdepth 1 -type d | tail -1)
        if [ -n "${LINUX_DIR}" ] && [ -f "${LINUX_DIR}/Makefile" ]; then
            T3_BUILD_START=$(date +%s%N)
            if ( cd "${LINUX_DIR}" && make defconfig > "${WORKDIR}/defconfig.log" 2>&1 ); then
                T3_BUILD_END=$(date +%s%N)
                T3_BUILD_MS=$(( (T3_BUILD_END - T3_BUILD_START) / 1000000 ))
                ok "make defconfig 成功 (${T3_BUILD_MS} ms)"
            else
                T3_BUILD_END=$(date +%s%N)
                T3_BUILD_MS=$(( (T3_BUILD_END - T3_BUILD_START) / 1000000 ))
                fail "make defconfig 失败 (${T3_BUILD_MS} ms) — 见 ${WORKDIR}/defconfig.log"
            fi
        else
            fail "未找到 Linux 源码目录或 Makefile"
        fi
    else
        fail "linux 源码 tar -xf 失败"
    fi
else
    # 退化为 powerfs-fuse make 测试 (轻量)
    if [ -d /app/powerfs ] && [ -f /app/powerfs/powerfs-fuse/Cargo.toml ]; then
        T3_BUILD_START=$(date +%s%N)
        # 仅 check 以避免完整编译耗时过长
        if ( cd /app/powerfs && cargo check --package powerfs-fuse > "${WORKDIR}/cargo_check.log" 2>&1 ); then
            T3_BUILD_END=$(date +%s%N)
            T3_BUILD_MS=$(( (T3_BUILD_END - T3_BUILD_START) / 1000000 ))
            ok "powerfs-fuse cargo check 成功 (${T3_BUILD_MS} ms) (轻量替代)"
        else
            T3_BUILD_END=$(date +%s%N)
            T3_BUILD_MS=$(( (T3_BUILD_END - T3_BUILD_START) / 1000000 ))
            fail "powerfs-fuse cargo check 失败 — 见 ${WORKDIR}/cargo_check.log"
        fi
    else
        skip "T2.3 源码编译 (未提供 LINUX_SRC 且无 /app/powerfs)"
    fi
fi

# ============================================================
echo ""
echo "--- T2.4 rsync -a 源码 → PowerFS ---"

# 检查 rsync 是否可用
if ! command -v rsync > /dev/null 2>&1; then
    skip "T2.4 rsync 未安装"
else
    RSYNC_DST="${TESTDIR}/rsync_dst"
    mkdir -p "${RSYNC_DST}"
    T4_START=$(date +%s%N)
    if rsync -a "${SRC_DIR}/" "${RSYNC_DST}/" 2>/dev/null; then
        T4_END=$(date +%s%N)
        T4_MS=$(( (T4_END - T4_START) / 1000000 ))
        ok "rsync -a 完成 (${T4_MS} ms)"
    else
        T4_END=$(date +%s%N)
        T4_MS=$(( (T4_END - T4_START) / 1000000 ))
        fail "rsync -a 失败 (${T4_MS} ms)"
    fi

    # rsync --checksum 不应有增量
    T4_CHECK_START=$(date +%s%N)
    RSYNC_OUT=$(rsync -a --checksum --dry-run --itemize-changes "${SRC_DIR}/" "${RSYNC_DST}/" 2>/dev/null || true)
    T4_CHECK_END=$(date +%s%N)
    T4_CHECK_MS=$(( (T4_CHECK_END - T4_CHECK_START) / 1000000 ))
    if [ -z "${RSYNC_OUT}" ]; then
        ok "rsync --checksum 无增量 (${T4_CHECK_MS} ms)"
    else
        fail "rsync --checksum 发现差异:"
        echo "${RSYNC_OUT}" | head -10 | sed 's/^/    /'
    fi
fi

# ============================================================
echo ""
echo "--- T2.5 git clone / git commit ---"

if ! command -v git > /dev/null 2>&1; then
    skip "T2.5 git 未安装"
else
    # 在 PowerFS 上 init 一个 git 仓库
    GIT_DIR="${TESTDIR}/git_repo"
    mkdir -p "${GIT_DIR}"
    T5_INIT_START=$(date +%s%N)
    if ( cd "${GIT_DIR}" && git init -q 2>&1 ); then
        T5_INIT_END=$(date +%s%N)
        T5_INIT_MS=$(( (T5_INIT_END - T5_INIT_START) / 1000000 ))
        ok "git init 成功 (${T5_INIT_MS} ms)"
    else
        T5_INIT_END=$(date +%s%N)
        T5_INIT_MS=$(( (T5_INIT_END - T5_INIT_START) / 1000000 ))
        fail "git init 失败 (${T5_INIT_MS} ms)"
    fi

    # 配置 user (测试环境无全局配置)
    ( cd "${GIT_DIR}" && git config user.email "test@powerfs.local" && git config user.name "PowerFS Test" ) 2>/dev/null

    # 创建文件并 commit
    echo "version 1" > "${GIT_DIR}/README.md"
    echo "content a" > "${GIT_DIR}/a.txt"
    mkdir -p "${GIT_DIR}/sub"
    echo "content b" > "${GIT_DIR}/sub/b.txt"

    T5_COMMIT_START=$(date +%s%N)
    if ( cd "${GIT_DIR}" && git add . && git commit -q -m "initial commit" 2>&1 ); then
        T5_COMMIT_END=$(date +%s%N)
        T5_COMMIT_MS=$(( (T5_COMMIT_END - T5_COMMIT_START) / 1000000 ))
        ok "git commit 成功 (${T5_COMMIT_MS} ms)"
    else
        T5_COMMIT_END=$(date +%s%N)
        T5_COMMIT_MS=$(( (T5_COMMIT_END - T5_COMMIT_START) / 1000000 ))
        fail "git commit 失败 (${T5_COMMIT_MS} ms)"
    fi

    # git status 应干净
    GIT_STATUS=$( cd "${GIT_DIR}" && git status --porcelain 2>/dev/null || echo "git-status-failed" )
    if [ -z "${GIT_STATUS}" ]; then
        ok "git status 干净"
    else
        fail "git status 非干净:"
        echo "${GIT_STATUS}" | head -10 | sed 's/^/    /'
    fi

    # 修改文件 + 新 commit + log
    echo "version 2" > "${GIT_DIR}/README.md"
    ( cd "${GIT_DIR}" && git add . && git commit -q -m "update README" 2>&1 ) || true
    GIT_LOG_COUNT=$( cd "${GIT_DIR}" && git log --oneline 2>/dev/null | wc -l )
    if [ "${GIT_LOG_COUNT}" -eq 2 ]; then
        ok "git log 显示 2 个提交"
    else
        fail "git log 提交数错误 (期望 2, 实际 ${GIT_LOG_COUNT})"
    fi

    # git checkout 回退到上一版本，文件内容应回退
    ( cd "${GIT_DIR}" && git checkout -q HEAD~1 README.md 2>&1 ) || true
    README_V1=$(cat "${GIT_DIR}/README.md")
    if [ "${README_V1}" = "version 1" ]; then
        ok "git checkout 回退正确"
    else
        fail "git checkout 回退错误 (期望 'version 1', 实际 '${README_V1}')"
    fi
fi

# ============================================================
echo ""
echo "--- 检查 fuse 日志异常 ---"
# 在容器内运行时检查 /var/log/powerfs-fuse.log
if [ -f /var/log/powerfs-fuse.log ]; then
    ERR_COUNT=$(grep -ciE 'error|panic|deadlock' /var/log/powerfs-fuse.log 2>/dev/null || echo 0)
    # 过滤已知无影响 warn (如 timeout 重试等)
    REAL_ERR=$(grep -iE 'error|panic|deadlock' /var/log/powerfs-fuse.log 2>/dev/null | grep -viE 'retry|timeout|eagain|would block' | head -5 || true)
    if [ -z "${REAL_ERR}" ]; then
        ok "无严重 error/panic/deadlock 日志"
    else
        fail "发现异常日志:"
        echo "${REAL_ERR}" | sed 's/^/    /'
    fi
else
    skip "无 /var/log/powerfs-fuse.log"
fi

# ============================================================
# 清理
echo ""
echo "--- 清理测试数据 ---"
rm -rf "${TESTDIR}" 2>/dev/null || true
rm -rf "${WORKDIR}" 2>/dev/null || true

# ============================================================
echo ""
echo "=========================================="
echo "PowerFS T2 文件系统正确性测试 完成"
echo "PASS: ${PASS}  FAIL: ${FAIL}  SKIP: ${SKIP}"
echo "时间: $(date)"
echo "=========================================="

if [ "${FAIL}" -gt 0 ]; then
    exit 1
fi
exit 0
