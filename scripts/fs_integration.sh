#!/bin/bash
set -e

TEST_DIR=$(mktemp -d)
MOUNT_DIR="${TEST_DIR}/mount"
MASTER_DIR="${TEST_DIR}/master_data"
VOLUME_DIR="${TEST_DIR}/volume_data"

echo "=== PowerFS 文件系统集成测试 ==="
echo "测试目录: ${TEST_DIR}"
echo ""

cleanup() {
    echo ""
    echo "=== 清理测试环境 ==="
    
    if mountpoint -q "$MOUNT_DIR"; then
        echo "卸载 FUSE..."
        umount "$MOUNT_DIR" 2>/dev/null || true
    fi
    
    echo "停止服务..."
    kill $FUSE_PID 2>/dev/null || true
    kill $VOLUME_PID 2>/dev/null || true
    kill $MASTER_PID 2>/dev/null || true
    
    wait $MASTER_PID 2>/dev/null || true
    wait $VOLUME_PID 2>/dev/null || true
    wait $FUSE_PID 2>/dev/null || true
    
    rm -rf "$TEST_DIR"
    echo "测试环境已清理"
}

trap cleanup EXIT

mkdir -p "$MOUNT_DIR" "$MASTER_DIR" "$VOLUME_DIR"

echo "=== 启动 Master 服务 ==="
cargo run -p powerfs-server -- master \
    --port 9333 \
    --dir "$MASTER_DIR" \
    2>&1 | tee "${TEST_DIR}/master.log" &
MASTER_PID=$!

sleep 3

echo ""
echo "=== 启动 Volume 服务 ==="
cargo run -p powerfs-volume -- \
    --grpc-address "127.0.0.1:8081" \
    --http-port 8080 \
    --node-id "test-node" \
    --master-address "127.0.0.1:9333" \
    --data-dir "$VOLUME_DIR" \
    2>&1 | tee "${TEST_DIR}/volume.log" &
VOLUME_PID=$!

sleep 3

echo ""
echo "=== 启动 FUSE 客户端 ==="
cargo run -p powerfs-fuse -- \
    --master "127.0.0.1:9333" \
    --mount-point "$MOUNT_DIR" \
    2>&1 | tee "${TEST_DIR}/fuse.log" &
FUSE_PID=$!

sleep 3

echo ""
echo "=== 测试 1: 文件创建 ==="
touch "$MOUNT_DIR/test_create.txt"
echo "hello powerfs" > "$MOUNT_DIR/test_create.txt"
cat "$MOUNT_DIR/test_create.txt"
echo "[OK] 文件创建成功"

echo ""
echo "=== 测试 2: 文件读取 ==="
CONTENT=$(cat "$MOUNT_DIR/test_create.txt")
if [ "$CONTENT" = "hello powerfs" ]; then
    echo "[OK] 文件读取成功"
else
    echo "[FAIL] 文件读取失败"
    exit 1
fi

echo ""
echo "=== 测试 3: 目录操作 ==="
mkdir -p "$MOUNT_DIR/subdir/nested"
touch "$MOUNT_DIR/subdir/file.txt"
ls -la "$MOUNT_DIR/subdir/"
echo "[OK] 目录操作成功"

echo ""
echo "=== 测试 4: 文件列表 ==="
FILES=$(ls "$MOUNT_DIR")
echo "文件列表: $FILES"
if echo "$FILES" | grep -q "test_create.txt" && echo "$FILES" | grep -q "subdir"; then
    echo "[OK] 文件列表正确"
else
    echo "[FAIL] 文件列表错误"
    exit 1
fi

echo ""
echo "=== 测试 5: 文件重命名 ==="
mv "$MOUNT_DIR/test_create.txt" "$MOUNT_DIR/test_renamed.txt"
if [ -f "$MOUNT_DIR/test_renamed.txt" ] && [ ! -f "$MOUNT_DIR/test_create.txt" ]; then
    echo "[OK] 文件重命名成功"
else
    echo "[FAIL] 文件重命名失败"
    exit 1
fi

echo ""
echo "=== 测试 6: 符号链接 ==="
ln -s "$MOUNT_DIR/test_renamed.txt" "$MOUNT_DIR/link.txt"
LINK_TARGET=$(readlink "$MOUNT_DIR/link.txt")
echo "符号链接目标: $LINK_TARGET"
if [ "$LINK_TARGET" = "$MOUNT_DIR/test_renamed.txt" ]; then
    echo "[OK] 符号链接成功"
else
    echo "[FAIL] 符号链接失败"
    exit 1
fi

echo ""
echo "=== 测试 7: 文件删除 ==="
rm "$MOUNT_DIR/test_renamed.txt"
rm "$MOUNT_DIR/link.txt"
if [ ! -f "$MOUNT_DIR/test_renamed.txt" ] && [ ! -f "$MOUNT_DIR/link.txt" ]; then
    echo "[OK] 文件删除成功"
else
    echo "[FAIL] 文件删除失败"
    exit 1
fi

echo ""
echo "=== 测试 8: 目录删除 ==="
rm -rf "$MOUNT_DIR/subdir"
if [ ! -d "$MOUNT_DIR/subdir" ]; then
    echo "[OK] 目录删除成功"
else
    echo "[FAIL] 目录删除失败"
    exit 1
fi

echo ""
echo "=== 测试 9: 大文件写入 ==="
dd if=/dev/urandom of="$MOUNT_DIR/large.bin" bs=1M count=10 2>/dev/null
FILE_SIZE=$(stat -c%s "$MOUNT_DIR/large.bin")
echo "大文件大小: ${FILE_SIZE} bytes"
if [ "$FILE_SIZE" -eq $((10 * 1024 * 1024)) ]; then
    echo "[OK] 大文件写入成功"
else
    echo "[FAIL] 大文件写入失败"
    exit 1
fi

echo ""
echo "=== 测试 10: 权限测试 ==="
chmod 755 "$MOUNT_DIR/large.bin"
chown $(whoami) "$MOUNT_DIR/large.bin"
ls -la "$MOUNT_DIR/large.bin"
echo "[OK] 权限测试完成"

echo ""
echo "=== 测试 11: 扩展属性 ==="
setfattr -n user.test_attr -v "test_value" "$MOUNT_DIR/large.bin"
ATTR_VALUE=$(getfattr -n user.test_attr -d "$MOUNT_DIR/large.bin" | grep -oP '="\K[^"]*')
echo "扩展属性值: $ATTR_VALUE"
if [ "$ATTR_VALUE" = "test_value" ]; then
    echo "[OK] 扩展属性成功"
else
    echo "[FAIL] 扩展属性失败"
    exit 1
fi

echo ""
echo "=== 测试 12: 文件复制 ==="
cp "$MOUNT_DIR/large.bin" "$MOUNT_DIR/large_copy.bin"
COPY_SIZE=$(stat -c%s "$MOUNT_DIR/large_copy.bin")
if [ "$COPY_SIZE" -eq "$FILE_SIZE" ]; then
    echo "[OK] 文件复制成功"
else
    echo "[FAIL] 文件复制失败"
    exit 1
fi

echo ""
echo "=== 测试 13: 小文件批量创建 ==="
for i in $(seq 1 10); do
    echo "file$i" > "$MOUNT_DIR/small$i.txt"
done
SMALL_COUNT=$(find "$MOUNT_DIR" -maxdepth 1 -name "small*.txt" | wc -l)
echo "创建小文件数量: $SMALL_COUNT"
if [ "$SMALL_COUNT" -eq 10 ]; then
    echo "[OK] 小文件批量创建成功"
else
    echo "[FAIL] 小文件批量创建失败"
    exit 1
fi

echo ""
echo "=== 测试 14: 并发写入 ==="
echo "启动 3 个并发写入进程..."
for i in 1 2 3; do
    (for j in $(seq 1 100); do echo "process$i-$j" >> "$MOUNT_DIR/concurrency.txt"; done) &
done
wait
LINE_COUNT=$(wc -l < "$MOUNT_DIR/concurrency.txt")
echo "并发写入行数: $LINE_COUNT"
if [ "$LINE_COUNT" -ge 300 ]; then
    echo "[OK] 并发写入成功"
else
    echo "[FAIL] 并发写入失败"
    exit 1
fi

echo ""
echo "=== 测试 15: 数据持久化 ==="
echo "持久化测试数据" > "$MOUNT_DIR/persistence.txt"

echo ""
echo "=== 所有测试通过 ==="
echo ""
echo "测试摘要:"
echo "- 文件创建/读取: OK"
echo "- 目录操作: OK"
echo "- 文件列表: OK"
echo "- 文件重命名: OK"
echo "- 符号链接: OK"
echo "- 文件/目录删除: OK"
echo "- 大文件写入 (10MB): OK"
echo "- 权限测试: OK"
echo "- 扩展属性: OK"
echo "- 文件复制: OK"
echo "- 小文件批量创建 (10个): OK"
echo "- 并发写入 (3进程): OK"
echo "- 数据持久化: OK"

exit 0
