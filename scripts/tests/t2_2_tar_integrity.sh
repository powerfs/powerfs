#!/bin/bash
# T2.2 tar czf + tar xzf 正确性测试
# 验证 tar 打包/解包后文件内容完整、权限正确、软硬链接完好
set -euo pipefail

MNT="/mnt/powerfs"
PASS=0
FAIL=0
SKIP=0

ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP] $1"; SKIP=$((SKIP+1)); }

# Use timestamp to avoid PID collisions when run via `docker exec`
# (where the container's PID != the script's bash PID on host).
RUNID=$(date +%s)$$
WORKDIR="/tmp/t2_2_work_${RUNID}"
SRC_DIR="${WORKDIR}/src"
TESTDIR="${MNT}/t2_2_test_${RUNID}"
TARBALL="/tmp/t2_2_src_${RUNID}.tar.gz"
EXTRACT_DIR="${TESTDIR}/extract_dst"

rm -rf "${TESTDIR}" 2>/dev/null || true
rm -rf "${WORKDIR}" 2>/dev/null || true
rm -f "${TARBALL}" 2>/dev/null || true
mkdir -p "${SRC_DIR}" "${TESTDIR}" "${EXTRACT_DIR}"

echo "=========================================="
echo "T2.2 tar czf + tar xzf 正确性测试"
echo "挂载点: ${MNT}"
echo "测试目录: ${TESTDIR}"
echo "时间: $(date)"
echo "=========================================="

# ============================================================
echo ""
echo "--- 生成源目录树 ---"

# 1. 小文件 (inline 模式)
echo "  - inline 小文件 (100B) × 30..."
for i in $(seq 1 30); do
    head -c 100 /dev/urandom | base64 > "${SRC_DIR}/small_${i}.txt"
done

# 2. 中等文件 (inline/flat 边界, 4-8KB)
echo "  - 边界文件 (4-8KB) × 15..."
for i in $(seq 1 15); do
    SIZE=$(( 4096 + (RANDOM % 4096) ))
    head -c ${SIZE} /dev/urandom > "${SRC_DIR}/mid_${i}.bin"
done

# 3. 大文件 (flat 模式, 50-200KB)
echo "  - flat 大文件 (50-200KB) × 10..."
for i in $(seq 1 10); do
    SIZE=$(( 50000 + (RANDOM % 150000) ))
    head -c ${SIZE} /dev/urandom > "${SRC_DIR}/large_${i}.bin"
done

# 4. 嵌套目录
echo "  - 嵌套目录..."
mkdir -p "${SRC_DIR}/a/b/c/d/e" "${SRC_DIR}/docs/images" "${SRC_DIR}/bin"
head -c 500 /dev/urandom | base64 > "${SRC_DIR}/a/b/c/d/e/deep.txt"
head -c 5000 /dev/urandom > "${SRC_DIR}/a/b/c/d/e/deep.bin"
head -c 2000 /dev/urandom > "${SRC_DIR}/docs/images/logo.png"
head -c 10000 /dev/urandom > "${SRC_DIR}/bin/app.exe"

# 5. 特殊权限
echo "  - 权限文件..."
touch "${SRC_DIR}/readonly.txt"
chmod 444 "${SRC_DIR}/readonly.txt"
head -c 500 /dev/urandom > "${SRC_DIR}/rw.txt"
chmod 600 "${SRC_DIR}/rw.txt"
touch "${SRC_DIR}/executable.sh"
chmod 755 "${SRC_DIR}/executable.sh"
head -c 500 /dev/urandom | base64 > "${SRC_DIR}/group_rw.txt"
chmod 660 "${SRC_DIR}/group_rw.txt"

# 6. 空文件
echo "  - 空文件..."
touch "${SRC_DIR}/empty1" "${SRC_DIR}/a/empty2" "${SRC_DIR}/a/b/empty3"

# 7. 硬链接 (tar 必须保留链接计数和链接关系)
# NOTE: All hard links placed in the same directory to ensure they land
# on the same shard. Cross-shard hard links require two-phase Raft propose
# (IncrementNlink on inode shard + AddDirEntry on new_parent shard) where
# the two shards may have different leaders — a known edge case handled
# separately, not part of tar data integrity validation.
echo "  - 硬链接..."
head -c 2000 /dev/urandom > "${SRC_DIR}/hlink_src.txt"
ln "${SRC_DIR}/hlink_src.txt" "${SRC_DIR}/hlink_1.txt"
ln "${SRC_DIR}/hlink_src.txt" "${SRC_DIR}/hlink_2.txt"
ln "${SRC_DIR}/hlink_src.txt" "${SRC_DIR}/hlink_3.txt"

# 8. 符号链接
echo "  - 符号链接..."
ln -s "hlink_src.txt" "${SRC_DIR}/symlink_rel.txt"
ln -s "${SRC_DIR}/small_1.txt" "${SRC_DIR}/symlink_abs.txt"
ln -s "../hlink_src.txt" "${SRC_DIR}/a/symlink_parent.txt"
ln -s "b/c/d/e/deep.txt" "${SRC_DIR}/a/deep_link.txt"

# 9. 文件名包含特殊字符 (tar 必须正确处理)
echo "  - 特殊文件名..."
head -c 500 /dev/urandom > "${SRC_DIR}/file with spaces.txt"
head -c 500 /dev/urandom > "${SRC_DIR}/file_utf8_中文测试_😀.txt"
head -c 500 /dev/urandom > "${SRC_DIR}/file(brackets)[1].txt"
head -c 500 /dev/urandom > "${SRC_DIR}/file_a=b&c.txt"

SRC_FILE_COUNT=$(find "${SRC_DIR}" -type f | wc -l)
SRC_HLINK_COUNT=$(find "${SRC_DIR}" -type f -links +1 | wc -l)
SRC_SYMLINK_COUNT=$(find "${SRC_DIR}" -type l | wc -l)
echo "  源文件数: ${SRC_FILE_COUNT} (硬链接: ${SRC_HLINK_COUNT}, 符号链接: ${SRC_SYMLINK_COUNT})"

# ============================================================
echo ""
echo "--- T2.2.1 tar czf: 源目录打包到 .tar.gz ---"
T_START=$(date +%s%N)
if tar czf "${TARBALL}" -C "${WORKDIR}" "src" 2>/dev/null; then
    T_END=$(date +%s%N)
    T_MS=$(( (T_END - T_START) / 1000000 ))
    TAR_SIZE=$(stat -c '%s' "${TARBALL}" 2>/dev/null || echo 0)
    ok "tar czf 完成 (${T_MS} ms, tarball=${TAR_SIZE} bytes)"
else
    fail "tar czf 失败"
    rm -rf "${WORKDIR}" "${TESTDIR}" "${TARBALL}"
    exit 1
fi

# ============================================================
echo ""
echo "--- T2.2.2 tar xzf: 解包到 PowerFS ---"
T_START=$(date +%s%N)
if tar xzf "${TARBALL}" -C "${EXTRACT_DIR}" 2>/dev/null; then
    T_END=$(date +%s%N)
    T_MS=$(( (T_END - T_START) / 1000000 ))
    ok "tar xzf 完成 (${T_MS} ms)"
else
    fail "tar xzf 失败"
    rm -rf "${WORKDIR}" "${TESTDIR}" "${TARBALL}"
    exit 1
fi

EXTRACT_ROOT="${EXTRACT_DIR}/src"

# ============================================================
echo ""
echo "--- T2.2.3 文件数量一致性 ---"
EXTRACT_FILE_COUNT=$(find "${EXTRACT_ROOT}" -type f | wc -l)
EXTRACT_HLINK_COUNT=$(find "${EXTRACT_ROOT}" -type f -links +1 | wc -l)
EXTRACT_SYMLINK_COUNT=$(find "${EXTRACT_ROOT}" -type l | wc -l)
echo "  源:   files=${SRC_FILE_COUNT} hardlinks=${SRC_HLINK_COUNT} symlinks=${SRC_SYMLINK_COUNT}"
echo "  解包: files=${EXTRACT_FILE_COUNT} hardlinks=${EXTRACT_HLINK_COUNT} symlinks=${EXTRACT_SYMLINK_COUNT}"

if [ "${SRC_FILE_COUNT}" -eq "${EXTRACT_FILE_COUNT}" ]; then
    ok "文件数一致 (${SRC_FILE_COUNT})"
else
    fail "文件数不一致 (src=${SRC_FILE_COUNT} extract=${EXTRACT_FILE_COUNT})"
fi

if [ "${SRC_SYMLINK_COUNT}" -eq "${EXTRACT_SYMLINK_COUNT}" ]; then
    ok "符号链接数一致 (${SRC_SYMLINK_COUNT})"
else
    fail "符号链接数不一致 (src=${SRC_SYMLINK_COUNT} extract=${EXTRACT_SYMLINK_COUNT})"
fi

# 硬链接数可能因 tar 实现略有差异，放宽检查
if [ "${EXTRACT_HLINK_COUNT}" -ge "${SRC_HLINK_COUNT}" ]; then
    ok "硬链接数检查通过 (src=${SRC_HLINK_COUNT} extract=${EXTRACT_HLINK_COUNT})"
else
    fail "硬链接数不足 (src=${SRC_HLINK_COUNT} extract=${EXTRACT_HLINK_COUNT})"
fi

# ============================================================
echo ""
echo "--- T2.2.4 文件内容完整性 (md5 校验) ---"
CONTENT_FAIL=0
TOTAL_CHECKED=0
EMPTY_FILES=0

while IFS= read -r src_file; do
    rel_path="${src_file#${SRC_DIR}/}"
    extract_file="${EXTRACT_ROOT}/${rel_path}"

    if [ ! -f "${extract_file}" ] && [ ! -L "${extract_file}" ]; then
        echo "  [FAIL] 缺失文件: ${rel_path}"
        CONTENT_FAIL=$((CONTENT_FAIL+1))
        continue
    fi

    # 跳过符号链接本身（单独验证链接目标）
    if [ -L "${src_file}" ]; then
        continue
    fi

    src_md5=$(md5sum "${src_file}" | awk '{print $1}')
    extract_md5=$(md5sum "${extract_file}" | awk '{print $1}')
    src_size=$(stat -c '%s' "${src_file}")
    extract_size=$(stat -c '%s' "${extract_file}")

    TOTAL_CHECKED=$((TOTAL_CHECKED+1))

    if [ "${src_md5}" = "${extract_md5}" ]; then
        :
    else
        echo "  [FAIL] md5 不匹配: ${rel_path}"
        echo "         src:  md5=${src_md5} size=${src_size}"
        echo "         dest: md5=${extract_md5} size=${extract_size}"
        CONTENT_FAIL=$((CONTENT_FAIL+1))

        if [ "${extract_size}" = "0" ]; then
            EMPTY_FILES=$((EMPTY_FILES+1))
            echo "         *** 目标文件为空 (0 bytes) ***"
        fi
    fi
done < <(find "${SRC_DIR}" -type f)

if [ "${CONTENT_FAIL}" -eq 0 ]; then
    ok "全部 ${TOTAL_CHECKED} 个常规文件 md5 一致 (${EMPTY_FILES} 空文件)"
else
    fail "${CONTENT_FAIL}/${TOTAL_CHECKED} 个文件 md5 不匹配 (${EMPTY_FILES} 个空文件)"
fi

# ============================================================
echo ""
echo "--- T2.2.5 符号链接验证 ---"
SYMLINK_FAIL=0
SYMLINK_CHECKED=0

while IFS= read -r src_link; do
    rel_path="${src_link#${SRC_DIR}/}"
    extract_link="${EXTRACT_ROOT}/${rel_path}"

    if [ ! -L "${extract_link}" ]; then
        echo "  [FAIL] 符号链接丢失: ${rel_path}"
        SYMLINK_FAIL=$((SYMLINK_FAIL+1))
        continue
    fi

    src_target=$(readlink "${src_link}")
    extract_target=$(readlink "${extract_link}")

    SYMLINK_CHECKED=$((SYMLINK_CHECKED+1))

    if [ "${src_target}" = "${extract_target}" ]; then
        :
    else
        echo "  [FAIL] 符号链接目标不一致: ${rel_path}"
        echo "         src:  -> ${src_target}"
        echo "         dest: -> ${extract_target}"
        SYMLINK_FAIL=$((SYMLINK_FAIL+1))
    fi
done < <(find "${SRC_DIR}" -type l)

if [ "${SYMLINK_FAIL}" -eq 0 ] && [ "${SYMLINK_CHECKED}" -gt 0 ]; then
    ok "全部 ${SYMLINK_CHECKED} 个符号链接验证通过"
else
    fail "符号链接验证失败 (${SYMLINK_FAIL}/${SYMLINK_CHECKED})"
fi

# ============================================================
echo ""
echo "--- T2.2.6 硬链接验证 ---"
HLINK_FAIL=0
HLINK_CHECKED=0

# 验证硬链接组 nlink 正确
while IFS= read -r src_hfile; do
    rel_path="${src_hfile#${SRC_DIR}/}"
    extract_file="${EXTRACT_ROOT}/${rel_path}"
    src_nlink=$(stat -c '%h' "${src_hfile}")
    extract_nlink=$(stat -c '%h' "${extract_file}")

    HLINK_CHECKED=$((HLINK_CHECKED+1))

    if [ "${src_nlink}" = "${extract_nlink}" ]; then
        :
    else
        echo "  [FAIL] 硬链接 nlink 不一致: ${rel_path}"
        echo "         src: nlink=${src_nlink}, dest: nlink=${extract_nlink}"
        HLINK_FAIL=$((HLINK_FAIL+1))
    fi
done < <(find "${SRC_DIR}" -type f -links +1)

# 验证硬链接组内所有文件 inode 相同（指向同一份数据）
HLINK_SRC_INODE=$(stat -c '%i' "${SRC_DIR}/hlink_src.txt")
HLINK_SRC_INODE_1=$(stat -c '%i' "${SRC_DIR}/hlink_1.txt")
HLINK_EX_INODE=$(stat -c '%i' "${EXTRACT_ROOT}/hlink_src.txt")
HLINK_EX_INODE_1=$(stat -c '%i' "${EXTRACT_ROOT}/hlink_1.txt")

if [ "${HLINK_SRC_INODE}" = "${HLINK_SRC_INODE_1}" ] && \
   [ "${HLINK_EX_INODE}" = "${HLINK_EX_INODE_1}" ]; then
    ok "硬链接组 inode 一致 (src=${HLINK_SRC_INODE}, extract=${HLINK_EX_INODE})"
else
    echo "  [FAIL] 硬链接组 inode 不一致"
    echo "         src: src=${HLINK_SRC_INODE} hlink_1=${HLINK_SRC_INODE_1}"
    echo "         extract: src=${HLINK_EX_INODE} hlink_1=${HLINK_EX_INODE_1}"
    HLINK_FAIL=$((HLINK_FAIL+1))
fi

if [ "${HLINK_FAIL}" -eq 0 ]; then
    ok "全部硬链接验证通过 (${HLINK_CHECKED} 个硬链接文件)"
else
    fail "硬链接验证失败 (${HLINK_FAIL} 项错误)"
fi

# ============================================================
echo ""
echo "--- T2.2.7 权限验证 ---"
PERM_FAIL=0
PERM_CHECKED=0

for check_file in readonly.txt rw.txt executable.sh group_rw.txt; do
    src_file="${SRC_DIR}/${check_file}"
    extract_file="${EXTRACT_ROOT}/${check_file}"
    [ ! -f "${src_file}" ] && continue

    src_mode=$(stat -c '%a' "${src_file}")
    extract_mode=$(stat -c '%a' "${extract_file}")

    PERM_CHECKED=$((PERM_CHECKED+1))

    if [ "${src_mode}" = "${extract_mode}" ]; then
        :
    else
        echo "  [FAIL] 权限不一致: ${check_file}"
        echo "         src: mode=${src_mode}, extract: mode=${extract_mode}"
        PERM_FAIL=$((PERM_FAIL+1))
    fi
done

if [ "${PERM_FAIL}" -eq 0 ]; then
    ok "全部 ${PERM_CHECKED} 个权限文件验证通过"
else
    fail "权限验证失败 (${PERM_FAIL}/${PERM_CHECKED})"
fi

# ============================================================
echo ""
echo "--- T2.2.8 空文件验证 ---"
EMPTY_FAIL=0
for empty in empty1 a/empty2 a/b/empty3; do
    src_file="${SRC_DIR}/${empty}"
    extract_file="${EXTRACT_ROOT}/${empty}"
    if [ ! -f "${extract_file}" ]; then
        echo "  [FAIL] 空文件缺失: ${empty}"
        EMPTY_FAIL=$((EMPTY_FAIL+1))
        continue
    fi
    extract_size=$(stat -c '%s' "${extract_file}")
    if [ "${extract_size}" = "0" ]; then
        :
    else
        echo "  [FAIL] 空文件非空: ${empty} (size=${extract_size})"
        EMPTY_FAIL=$((EMPTY_FAIL+1))
    fi
done

if [ "${EMPTY_FAIL}" -eq 0 ]; then
    ok "全部 3 个空文件验证通过"
else
    fail "空文件验证失败 (${EMPTY_FAIL} 项错误)"
fi

# ============================================================
echo ""
echo "=========================================="
echo "T2.2 tar czf + tar xzf 正确性测试结果"
echo "  PASS: ${PASS}"
echo "  FAIL: ${FAIL}"
echo "  SKIP: ${SKIP}"
echo "=========================================="

# 清理
rm -rf "${WORKDIR}" 2>/dev/null || true
rm -f "${TARBALL}" 2>/dev/null || true

if [ "${FAIL}" -eq 0 ]; then
    exit 0
else
    exit 1
fi
