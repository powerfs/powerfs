#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

TEST_DIR="/mnt/powerfs/test_correctness_$(date +%Y%m%d_%H%M%S)"
FAILED=0
PASSED=0

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASSED=$((PASSED + 1))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    FAILED=$((FAILED + 1))
}

log_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

run_test() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"
    
    if [ "$expected" == "$actual" ]; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
        echo "  Expected: '$expected'"
        echo "  Actual:   '$actual'"
    fi
}

run_test_contains() {
    local test_name="$1"
    local pattern="$2"
    local actual="$3"
    
    if [[ "$actual" == *"$pattern"* ]]; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
        echo "  Expected to contain: '$pattern'"
        echo "  Actual:              '$actual'"
    fi
}

log_info "=== Phase 1: 单客户端基础操作测试 ==="

log_info "创建测试目录..."
docker exec fuse-1 mkdir -p "$TEST_DIR"
DIR_LIST=$(docker exec fuse-1 ls /mnt/powerfs 2>/dev/null)
run_test_contains "创建目录" "test_correctness_" "$DIR_LIST"

log_info "创建文件..."
docker exec fuse-1 bash -c "echo 'hello world' > $TEST_DIR/test1.txt"
CONTENT=$(docker exec fuse-1 cat "$TEST_DIR/test1.txt" 2>/dev/null || echo "")
run_test "创建文件" "hello world" "$CONTENT"

log_info "追加写入..."
docker exec fuse-1 bash -c "echo 'line2' >> $TEST_DIR/test1.txt" 2>/dev/null || true
CONTENT=$(docker exec fuse-1 cat "$TEST_DIR/test1.txt" 2>/dev/null || echo "")
run_test "追加写入" "hello world
line2" "$CONTENT"

log_info "覆盖写入..."
docker exec fuse-1 bash -c "echo 'overwritten' > $TEST_DIR/test1.txt" 2>/dev/null || true
CONTENT=$(docker exec fuse-1 cat "$TEST_DIR/test1.txt" 2>/dev/null || echo "")
run_test "覆盖写入" "overwritten" "$CONTENT"

log_info "写入大文件..."
docker exec fuse-1 bash -c "head -c 10M /dev/urandom > $TEST_DIR/bigfile.txt" 2>/dev/null || true
sleep 1
FILE_SIZE=$(docker exec fuse-1 stat -c%s "$TEST_DIR/bigfile.txt" 2>/dev/null || echo 0)
run_test "写入大文件" "OK" "$([ $FILE_SIZE -gt 10000000 ] && echo 'OK' || echo 'FAIL')"

log_info "读取大文件..."
MD5_BEFORE=$(docker exec fuse-1 md5sum "$TEST_DIR/bigfile.txt" 2>/dev/null | cut -d' ' -f1 || echo "")
run_test "读取大文件" "OK" "$([ -n "$MD5_BEFORE" ] && echo 'OK' || echo 'FAIL')"

log_info "创建嵌套目录..."
docker exec fuse-1 mkdir -p "$TEST_DIR/subdir/nested" 2>/dev/null || true
RESULT=$(docker exec fuse-1 ls "$TEST_DIR/subdir" 2>/dev/null || echo "")
run_test "创建嵌套目录" "nested" "$RESULT"

log_info "在子目录创建文件..."
docker exec fuse-1 bash -c "echo 'nested file' > $TEST_DIR/subdir/nested/file.txt" 2>/dev/null || true
CONTENT=$(docker exec fuse-1 cat "$TEST_DIR/subdir/nested/file.txt" 2>/dev/null || echo "")
run_test "子目录文件创建" "nested file" "$CONTENT"

log_info "重命名文件..."
docker exec fuse-1 mv "$TEST_DIR/test1.txt" "$TEST_DIR/renamed.txt" 2>/dev/null || true
CONTENT=$(docker exec fuse-1 cat "$TEST_DIR/renamed.txt" 2>/dev/null || echo "")
run_test "文件重命名" "overwritten" "$CONTENT"

log_info "重命名目录..."
docker exec fuse-1 mv "$TEST_DIR/subdir" "$TEST_DIR/renamed_subdir" 2>/dev/null || true
CONTENT=$(docker exec fuse-1 cat "$TEST_DIR/renamed_subdir/nested/file.txt" 2>/dev/null || echo "")
run_test "目录重命名" "nested file" "$CONTENT"

log_info "删除文件..."
docker exec fuse-1 rm "$TEST_DIR/renamed.txt" 2>/dev/null || true
RESULT=$(docker exec fuse-1 ls "$TEST_DIR" 2>/dev/null || true)
run_test "删除文件" "OK" "$([[ "$RESULT" != *renamed.txt* ]] && echo 'OK' || echo 'FAIL')"

log_info "递归删除目录..."
docker exec fuse-1 rm -rf "$TEST_DIR/renamed_subdir" 2>/dev/null || true
RESULT=$(docker exec fuse-1 ls "$TEST_DIR" 2>/dev/null || true)
run_test "递归删除目录" "OK" "$([[ "$RESULT" != *renamed_subdir* ]] && echo 'OK' || echo 'FAIL')"

log_info "=== Phase 2: 目录操作测试 ==="

log_info "创建源测试目录..."
docker exec fuse-1 bash -c "mkdir -p /tmp/test_src && echo 'file1 content' > /tmp/test_src/file1.txt && echo 'file2 content' > /tmp/test_src/file2.txt && mkdir -p /tmp/test_src/sub && echo 'sub file' > /tmp/test_src/sub/file3.txt && mkdir -p /tmp/test_src/sub/nested && echo 'nested file' > /tmp/test_src/sub/nested/file4.txt"

log_info "拷贝目录..."
docker exec fuse-1 bash -c "cp -r /tmp/test_src $TEST_DIR/test_src_copy" 2>/dev/null || true
RESULT=$(docker exec fuse-1 ls "$TEST_DIR/test_src_copy" 2>/dev/null | head -1 || echo "")
run_test "目录拷贝" "OK" "$([ -n "$RESULT" ] && echo 'OK' || echo 'FAIL')"

log_info "验证目录内容..."
FILE1=$(docker exec fuse-1 cat "$TEST_DIR/test_src_copy/file1.txt" 2>/dev/null || echo "")
FILE2=$(docker exec fuse-1 cat "$TEST_DIR/test_src_copy/file2.txt" 2>/dev/null || echo "")
FILE3=$(docker exec fuse-1 cat "$TEST_DIR/test_src_copy/sub/file3.txt" 2>/dev/null || echo "")
FILE4=$(docker exec fuse-1 cat "$TEST_DIR/test_src_copy/sub/nested/file4.txt" 2>/dev/null || echo "")
if [ "$FILE1" == "file1 content" ] && [ "$FILE2" == "file2 content" ] && [ "$FILE3" == "sub file" ] && [ "$FILE4" == "nested file" ]; then
    log_pass "目录内容验证"
else
    log_fail "目录内容验证"
    echo "  file1: '$FILE1'"
    echo "  file2: '$FILE2'"
    echo "  file3: '$FILE3'"
    echo "  file4: '$FILE4'"
fi

log_info "创建压缩包..."
docker exec fuse-1 bash -c "cd /tmp && tar czf test_src.tar.gz test_src"

log_info "拷贝压缩包到 PowerFS..."
docker exec fuse-1 bash -c "cp /tmp/test_src.tar.gz $TEST_DIR/test_src.tar.gz"
RESULT=$(docker exec fuse-1 ls "$TEST_DIR" 2>/dev/null | grep test_src.tar.gz || echo "")
run_test "压缩包拷贝" "OK" "$([ -n "$RESULT" ] && echo 'OK' || echo 'FAIL')"

log_info "解压缩到 PowerFS..."
docker exec fuse-1 bash -c "cd $TEST_DIR && tar xzf test_src.tar.gz"
RESULT=$(docker exec fuse-1 ls "$TEST_DIR/test_src" 2>/dev/null | head -1 || echo "")
run_test "解压缩" "OK" "$([ -n "$RESULT" ] && echo 'OK' || echo 'FAIL')"

log_info "验证解压缩内容..."
DECOMPRESSED=$(docker exec fuse-1 cat "$TEST_DIR/test_src/file1.txt" 2>/dev/null || echo "")
run_test "解压缩内容验证" "file1 content" "$DECOMPRESSED"

log_info "=== Phase 3: rm -rf 重建目录测试 ==="

log_info "删除测试目录..."
docker exec fuse-1 rm -rf "$TEST_DIR/test_src" "$TEST_DIR/test_src_copy" 2>/dev/null || true
sleep 2
RESULT=$(docker exec fuse-1 ls "$TEST_DIR" 2>/dev/null || echo "")
run_test "删除目录" "OK" "$([[ "$RESULT" != *test_src ]] && echo 'OK' || echo 'FAIL')"

log_info "重新创建目录..."
docker exec fuse-1 bash -c "cp -r /tmp/test_src $TEST_DIR/test_src" 2>/dev/null || true
RESULT=$(docker exec fuse-1 ls "$TEST_DIR/test_src" 2>/dev/null | head -1 || echo "")
run_test "重新创建目录" "OK" "$([ -n "$RESULT" ] && echo 'OK' || echo 'FAIL')"

log_info "验证重建目录内容..."
CONTENT=$(docker exec fuse-1 cat "$TEST_DIR/test_src/file1.txt" 2>/dev/null || echo "")
run_test "重建目录内容验证" "file1 content" "$CONTENT"

log_info "多次删除重建..."
for i in 1 2 3; do
    docker exec fuse-1 rm -rf "$TEST_DIR/test_src" 2>/dev/null || true
    sleep 0.5
    docker exec fuse-1 bash -c "cp -r /tmp/test_src $TEST_DIR/test_src" 2>/dev/null || true
done
RESULT=$(docker exec fuse-1 ls "$TEST_DIR/test_src" 2>/dev/null | head -1 || echo "")
run_test "多次删除重建" "OK" "$([ -n "$RESULT" ] && echo 'OK' || echo 'FAIL')"

log_info "=== Phase 4: 大目录拷贝测试 ==="

log_info "创建大测试目录..."
docker exec fuse-1 bash -c "mkdir -p /tmp/big_test && for i in \$(seq 1 10); do echo \"file \$i content\" > /tmp/big_test/file\$i.txt; done && mkdir -p /tmp/big_test/subdir1/subdir2 && for i in \$(seq 1 5); do echo \"sub file \$i\" > /tmp/big_test/subdir1/subdir2/subfile\$i.txt; done"

log_info "拷贝大目录..."
docker exec fuse-1 bash -c "rm -rf $TEST_DIR/big_test && sleep 3 && cp -r /tmp/big_test $TEST_DIR/big_test" 2>/dev/null || true
FILE_COUNT=$(docker exec fuse-1 find "$TEST_DIR/big_test" -type f | wc -l 2>/dev/null || echo 0)
run_test "大目录拷贝" "OK" "$([ $FILE_COUNT -eq 15 ] && echo 'OK' || echo 'FAIL')"

log_info "删除大目录..."
docker exec fuse-1 rm -rf "$TEST_DIR/big_test" 2>/dev/null || true
sleep 1
RESULT=$(docker exec fuse-1 ls "$TEST_DIR" 2>/dev/null || echo "")
run_test "删除大目录" "OK" "$([[ "$RESULT" != *big_test* ]] && echo 'OK' || echo 'FAIL')"

log_info "=== Phase 5: 跨客户端一致性测试 ==="

log_info "fuse-1 创建文件..."
docker exec fuse-1 bash -c "echo 'cross-client test content' > $TEST_DIR/cross_test.txt" 2>/dev/null || true
MD5_FUSE1=$(docker exec fuse-1 md5sum "$TEST_DIR/cross_test.txt" 2>/dev/null | cut -d' ' -f1 || echo "")
run_test "fuse-1 创建文件" "OK" "$([ -n "$MD5_FUSE1" ] && echo 'OK' || echo 'FAIL')"

log_info "等待缓存失效传播..."
sleep 3

log_info "fuse-2 读取文件..."
MD5_FUSE2=$(docker exec fuse-2 md5sum "$TEST_DIR/cross_test.txt" 2>/dev/null | cut -d' ' -f1 || echo "")
run_test "fuse-2 读取一致性" "$MD5_FUSE1" "$MD5_FUSE2"

log_info "fuse-3 读取文件..."
MD5_FUSE3=$(docker exec fuse-3 md5sum "$TEST_DIR/cross_test.txt" 2>/dev/null | cut -d' ' -f1 || echo "")
run_test "fuse-3 读取一致性" "$MD5_FUSE1" "$MD5_FUSE3"

log_info "fuse-2 追加写入..."
docker exec fuse-2 bash -c "echo 'fuse-2 appends' >> $TEST_DIR/cross_test.txt" 2>/dev/null || true
sleep 3

log_info "fuse-1 验证追加内容..."
CONTENT=$(docker exec fuse-1 cat "$TEST_DIR/cross_test.txt" 2>/dev/null || echo "")
EXPECTED="cross-client test content
fuse-2 appends"
run_test "fuse-1 验证追加" "$EXPECTED" "$CONTENT"

log_info "fuse-3 验证追加内容..."
CONTENT=$(docker exec fuse-3 cat "$TEST_DIR/cross_test.txt" 2>/dev/null || echo "")
run_test "fuse-3 验证追加" "$EXPECTED" "$CONTENT"

log_info "=== Phase 6: 跨客户端目录一致性测试 ==="

log_info "fuse-1 创建目录..."
docker exec fuse-1 bash -c "mkdir -p $TEST_DIR/cross_dir && echo 'dir file' > $TEST_DIR/cross_dir/file.txt" 2>/dev/null || true
sleep 2

log_info "fuse-2 验证目录..."
CONTENT=$(docker exec fuse-2 cat "$TEST_DIR/cross_dir/file.txt" 2>/dev/null || echo "")
run_test "fuse-2 验证目录" "dir file" "$CONTENT"

log_info "fuse-3 删除目录..."
docker exec fuse-3 rm -rf "$TEST_DIR/cross_dir" 2>/dev/null || true
sleep 2

log_info "fuse-1 验证目录删除..."
RESULT=$(docker exec fuse-1 ls "$TEST_DIR" 2>/dev/null || echo "")
run_test "fuse-1 验证目录删除" "OK" "$([[ "$RESULT" != *cross_dir* ]] && echo 'OK' || echo 'FAIL')"

log_info "=== 清理测试目录 ==="
docker exec fuse-1 rm -rf "$TEST_DIR" 2>/dev/null || true

echo ""
echo "=== 测试结果汇总 ==="
echo "通过: $PASSED"
echo "失败: $FAILED"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}所有测试通过!${NC}"
    exit 0
else
    echo -e "${RED}有 $FAILED 个测试失败!${NC}"
    exit 1
fi