#!/bin/bash

FUSE_CONTAINER="fuse-1"
TEST_DIR="/mnt/powerfs/test_minimal_$(date +%Y%m%d_%H%M%S)"

log_info() {
    echo "[INFO] $1"
}

log_pass() {
    echo "[PASS] $1"
}

log_fail() {
    echo "[FAIL] $1"
    exit 1
}

run_test() {
    local name="$1"
    local expected="$2"
    local actual="$3"
    
    if [ "$expected" = "$actual" ] || echo "$actual" | grep -q "$expected"; then
        log_pass "$name"
    else
        log_fail "$name (expected: '$expected', got: '$actual')"
    fi
}

log_info "=== 极简单客户端测试 ==="

log_info "测试1: 创建目录"
docker exec $FUSE_CONTAINER mkdir -p "$TEST_DIR" 2>/dev/null || true
DIR_EXISTS=$(docker exec $FUSE_CONTAINER ls /mnt/powerfs | grep -c "test_minimal_")
run_test "创建目录" "1" "$DIR_EXISTS"

log_info "测试2: 创建文件"
docker exec $FUSE_CONTAINER bash -c "echo 'hello world' > $TEST_DIR/file.txt"
FILE_CONTENT=$(docker exec $FUSE_CONTAINER cat "$TEST_DIR/file.txt" 2>/dev/null || echo "")
run_test "创建文件" "hello world" "$FILE_CONTENT"

log_info "测试3: 追加写入"
docker exec $FUSE_CONTAINER bash -c "echo 'line2' >> $TEST_DIR/file.txt"
FILE_CONTENT=$(docker exec $FUSE_CONTAINER cat "$TEST_DIR/file.txt" 2>/dev/null || echo "")
EXPECTED="hello world
line2"
run_test "追加写入" "$EXPECTED" "$FILE_CONTENT"

log_info "测试4: 覆盖写入"
docker exec $FUSE_CONTAINER bash -c "echo 'overwritten' > $TEST_DIR/file.txt"
FILE_CONTENT=$(docker exec $FUSE_CONTAINER cat "$TEST_DIR/file.txt" 2>/dev/null || echo "")
run_test "覆盖写入" "overwritten" "$FILE_CONTENT"

log_info "测试5: 创建子目录"
docker exec $FUSE_CONTAINER mkdir -p "$TEST_DIR/subdir" 2>/dev/null || true
SUBDIR_EXISTS=$(docker exec $FUSE_CONTAINER ls "$TEST_DIR" | grep -c "subdir")
run_test "创建子目录" "1" "$SUBDIR_EXISTS"

log_info "测试6: 子目录创建文件"
docker exec $FUSE_CONTAINER bash -c "echo 'nested file' > $TEST_DIR/subdir/nested.txt"
NESTED_CONTENT=$(docker exec $FUSE_CONTAINER cat "$TEST_DIR/subdir/nested.txt" 2>/dev/null || echo "")
run_test "子目录文件" "nested file" "$NESTED_CONTENT"

log_info "测试7: 文件重命名"
docker exec $FUSE_CONTAINER mv "$TEST_DIR/file.txt" "$TEST_DIR/renamed.txt" 2>/dev/null || true
RENAMED_EXISTS=$(docker exec $FUSE_CONTAINER ls "$TEST_DIR" | grep -c "renamed.txt" || true)
run_test "文件重命名" "1" "$RENAMED_EXISTS"

log_info "测试8: 删除文件"
docker exec $FUSE_CONTAINER rm "$TEST_DIR/renamed.txt" 2>/dev/null || true
FILE_DELETED=$(docker exec $FUSE_CONTAINER ls "$TEST_DIR" | grep -c "renamed.txt" || true)
run_test "删除文件" "0" "$FILE_DELETED"

log_info "测试9: 删除子目录"
docker exec $FUSE_CONTAINER rmdir "$TEST_DIR/subdir" 2>/dev/null || true
SUBDIR_DELETED=$(docker exec $FUSE_CONTAINER ls "$TEST_DIR" | grep -c "subdir" || true)
run_test "删除子目录" "0" "$SUBDIR_DELETED"

log_info "测试10: 删除测试目录"
docker exec $FUSE_CONTAINER rmdir "$TEST_DIR" 2>/dev/null || true
DIR_DELETED=$(docker exec $FUSE_CONTAINER ls /mnt/powerfs | grep -c "test_minimal_" || true)
run_test "删除测试目录" "0" "$DIR_DELETED"

log_info ""
log_info "=== 所有测试通过 ==="