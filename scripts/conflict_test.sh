#!/bin/bash

set -e

MOUNT1="/tmp/powerfs-posix-test"
MOUNT2="/tmp/powerfs-posix-test2"
MASTER="127.0.0.1:36697"

echo "=== PowerFS 冲突测试脚本 ==="
echo ""

echo "[测试1] 创建大量冲突文件..."
echo "----------------------------------------"
mkdir -p "$MOUNT1/conflict-mass" "$MOUNT2/conflict-mass"

# 客户端1创建10个文件
for i in $(seq 1 10); do
    echo "client1_file$i" > "$MOUNT1/conflict-mass/file$i.txt"
done
echo "客户端1: 创建 10 个文件 ✅"

sleep 1

# 客户端2创建同名文件（触发冲突）
for i in $(seq 1 10); do
    echo "client2_file$i" > "$MOUNT2/conflict-mass/file$i.txt"
done
echo "客户端2: 创建 10 个同名文件 ✅"

sleep 2
echo "等待冲突检测..."
sleep 3

echo ""
echo "[测试2] 双客户端解压目录..."
echo "----------------------------------------"
mkdir -p /tmp/test-tarball
cd /tmp/test-tarball || exit

# 创建测试目录结构
mkdir -p dir1/subdir1 dir1/subdir2 dir2
echo "file1 content" > dir1/subdir1/file1.txt
echo "file2 content" > dir1/subdir2/file2.txt
echo "file3 content" > dir2/file3.txt
echo "root content" > root.txt

# 创建tarball
tar -czf test.tar.gz .
echo "创建测试 tarball ✅"

# 复制到两个客户端
cp test.tar.gz "$MOUNT1/"
cp test.tar.gz "$MOUNT2/"

# 客户端1解压
cd "$MOUNT1" || exit
tar -xzf test.tar.gz &
PID1=$!

# 客户端2同时解压
cd "$MOUNT2" || exit
tar -xzf test.tar.gz &
PID2=$!

echo "双客户端同时解压..."
wait $PID1 $PID2
echo "双客户端解压完成 ✅"

sleep 3

echo ""
echo "[测试3] 目录创建冲突..."
echo "----------------------------------------"
mkdir -p "$MOUNT1/conflict-dir" "$MOUNT2/conflict-dir"

# 同时创建同名子目录
mkdir "$MOUNT1/conflict-dir/same-name" &
mkdir "$MOUNT2/conflict-dir/same-name" &
wait
echo "双客户端创建同名目录 ✅"

sleep 2

echo ""
echo "[测试4] 验证结果..."
echo "----------------------------------------"
echo "冲突测试目录内容:"
ls -la "$MOUNT1/conflict-mass/" | head -15
echo ""

echo "解压目录内容:"
ls -la "$MOUNT1/dir1/" 2>/dev/null || echo "dir1 不存在"
ls -la "$MOUNT2/dir1/" 2>/dev/null || echo "dir1 不存在"
echo ""

echo "目录创建冲突结果:"
ls -la "$MOUNT1/conflict-dir/" 2>/dev/null || echo "conflict-dir 不存在"
echo ""

echo "[测试5] CLI 冲突检测..."
echo "----------------------------------------"
cd /home/portion/powerfs || exit
./target/release/powerfs-cli -m "$MASTER" conflicts list --path / 2>/dev/null | head -30 || echo "冲突列表查询完成"

echo ""
echo "=== 测试完成 ==="
