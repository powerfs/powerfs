#!/bin/bash
set -e

TEST_DIR1="/tmp/powerfs-posix-test/conflict-test"
TEST_DIR2="/tmp/powerfs-posix-test2/conflict-test"
CLI_BIN="/home/portion/powerfs/target/release/powerfs-cli"
MASTER_ADDR="127.0.0.1:36697"

echo "=== PowerFS 冲突测试脚本 ==="
echo "测试目录1: $TEST_DIR1"
echo "测试目录2: $TEST_DIR2"
echo "CLI 路径: $CLI_BIN -m $MASTER_ADDR"
echo ""

if ! mount | grep -q "powerfs"; then
    echo "[错误] PowerFS 未挂载，请先挂载两个客户端"
    exit 1
fi

mkdir -p "$TEST_DIR1"
mkdir -p "$TEST_DIR2"

RESULT_FILE="/tmp/conflict_test_results.txt"
> "$RESULT_FILE"

pass_count=0
fail_count=0

run_test() {
    local name="$1"
    local expected="$2"
    shift 2
    
    echo -n "[测试] $name ... "
    
    if "$@"; then
        echo "✅ 通过"
        echo "$name: PASS" >> "$RESULT_FILE"
        ((pass_count++))
    else
        echo "❌ 失败"
        echo "$name: FAIL" >> "$RESULT_FILE"
        ((fail_count++))
    fi
}

echo "=== 场景 1: CreateCreate + 内容相同（自动合并）==="
run_test "设置 aggressive 策略" \
    "$CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy aggressive" \
    $CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy aggressive

rm -f "$TEST_DIR1/scene1.txt" "$TEST_DIR2/scene1.txt"
sync
sleep 0.2

echo "hello world" > "$TEST_DIR1/scene1.txt"
echo "hello world" > "$TEST_DIR2/scene1.txt"
sync
sleep 0.5

run_test "客户端1文件应存在" \
    "[ -f $TEST_DIR1/scene1.txt ]" \
    [ -f "$TEST_DIR1/scene1.txt" ]

run_test "客户端2文件应存在" \
    "[ -f $TEST_DIR2/scene1.txt ]" \
    [ -f "$TEST_DIR2/scene1.txt" ]

run_test "文件内容应相同" \
    "$(cat $TEST_DIR1/scene1.txt) == $(cat $TEST_DIR2/scene1.txt)" \
    [ "$(cat "$TEST_DIR1/scene1.txt")" = "$(cat "$TEST_DIR2/scene1.txt")" ]

echo ""

echo "=== 场景 2: CreateCreate + 内容不同（LWW）==="
rm -f "$TEST_DIR1/scene2.txt" "$TEST_DIR2/scene2.txt"
sync
sleep 0.2

echo "client1" > "$TEST_DIR1/scene2.txt"
sleep 0.5
echo "client2" > "$TEST_DIR2/scene2.txt"
sync
sleep 0.5

run_test "文件内容应为 client2（LWW）" \
    "$(cat $TEST_DIR1/scene2.txt) == 'client2'" \
    [ "$(cat "$TEST_DIR1/scene2.txt")" = "client2" ]

echo ""

echo "=== 场景 3: WriteUnlink + WritePriority ==="
rm -f "$TEST_DIR1/scene3.txt" "$TEST_DIR2/scene3.txt"
sync
sleep 0.2

echo "test" > "$TEST_DIR1/scene3.txt"
$CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy write-priority

echo "updated" > "$TEST_DIR1/scene3.txt"
rm "$TEST_DIR2/scene3.txt" 2>/dev/null || true
sync
sleep 0.5

run_test "文件应保留（WritePriority）" \
    "[ -f $TEST_DIR1/scene3.txt ]" \
    [ -f "$TEST_DIR1/scene3.txt" ]

echo ""

echo "=== 场景 4: WriteUnlink + DeletePriority ==="
rm -f "$TEST_DIR1/scene4.txt" "$TEST_DIR2/scene4.txt"
sync
sleep 0.2

echo "test" > "$TEST_DIR1/scene4.txt"
$CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy delete-priority

echo "updated" > "$TEST_DIR1/scene4.txt"
rm "$TEST_DIR2/scene4.txt" 2>/dev/null || true
sync
sleep 0.5

run_test "文件应删除（DeletePriority）" \
    "[ ! -f $TEST_DIR1/scene4.txt ]" \
    [ ! -f "$TEST_DIR1/scene4.txt" ]

echo ""

echo "=== 场景 5: DeleteCreate 冲突 ==="
rm -f "$TEST_DIR1/scene5.txt" "$TEST_DIR2/scene5.txt"
sync
sleep 0.2

echo "original" > "$TEST_DIR1/scene5.txt"
rm "$TEST_DIR2/scene5.txt"
echo "recreated" > "$TEST_DIR1/scene5.txt"
sync
sleep 0.5

run_test "文件应存在" \
    "[ -f $TEST_DIR1/scene5.txt ]" \
    [ -f "$TEST_DIR1/scene5.txt" ]

run_test "文件内容应为 recreated" \
    "$(cat $TEST_DIR1/scene5.txt) == 'recreated'" \
    [ "$(cat "$TEST_DIR1/scene5.txt")" = "recreated" ]

echo ""

echo "=== 场景 6: CLI 冲突列表 ==="
$CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy manual
echo "client1" > "$TEST_DIR1/cli.txt"
echo "client2" > "$TEST_DIR2/cli.txt"
sync
sleep 0.5

run_test "CLI list 应成功" \
    "$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test" \
    $CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test > /dev/null

echo ""

echo "=== 场景 7: CLI 自动解决 ==="
run_test "CLI auto-resolve 应成功" \
    "$CLI_BIN -m $MASTER_ADDR conflicts auto-resolve --path /conflict-test --policy aggressive" \
    $CLI_BIN -m $MASTER_ADDR conflicts auto-resolve --path /conflict-test --policy aggressive

echo ""

echo "=== 场景 8: .conflicts/ 目录 ==="
run_test "冲突目录应可访问" \
    "[ -d $TEST_DIR1/.conflicts ]" \
    [ -d "$TEST_DIR1/.conflicts" ]

echo ""

echo "=== 测试结果汇总 ==="
echo "通过: $pass_count"
echo "失败: $fail_count"
echo ""

if [ $fail_count -eq 0 ]; then
    echo "🎉 所有测试通过！"
    exit 0
else
    echo "⚠️ 部分测试失败，请检查日志"
    exit 1
fi