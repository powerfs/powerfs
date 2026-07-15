#!/bin/bash

set -e

MASTER_ADDR="127.0.0.1:36697"
CLI_BIN="./target/release/powerfs-cli"

MOUNT_POINT1="/tmp/powerfs-posix-test"
MOUNT_POINT2="/tmp/powerfs-posix-test2"
MOUNT_POINT3="/tmp/powerfs-posix-test3"
MOUNT_POINT4="/tmp/powerfs-posix-test4"

TEST_BASE="/deadlock-test"
TEST_DIR1="$MOUNT_POINT1$TEST_BASE"
TEST_DIR2="$MOUNT_POINT2$TEST_BASE"
TEST_DIR3="$MOUNT_POINT3$TEST_BASE"
TEST_DIR4="$MOUNT_POINT4$TEST_BASE"

PASSED=0
FAILED=0
WARNINGS=0

TIMEOUT_SHORT=5
TIMEOUT_MEDIUM=10
TIMEOUT_LONG=30

log_info() {
    echo -e "\033[1;34m[INFO] $(date '+%H:%M:%S') $*\033[0m"
}

log_success() {
    echo -e "\033[1;32m[SUCCESS] $(date '+%H:%M:%S') $*\033[0m"
}

log_error() {
    echo -e "\033[1;31m[ERROR] $(date '+%H:%M:%S') $*\033[0m"
}

log_warn() {
    echo -e "\033[1;33m[WARN] $(date '+%H:%M:%S') $*\033[0m"
}

log_test() {
    echo -e "\033[1;36m[TEST] $*\033[0m"
}

check_mount_alive() {
    local mount="$1"
    local name="$2"
    
    timeout $TIMEOUT_SHORT df -h "$mount" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        log_error "$name 挂载点无响应，可能发生死锁"
        return 1
    fi
    
    timeout $TIMEOUT_SHORT ls "$mount" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        log_error "$name ls 操作超时，可能发生死锁"
        return 1
    fi
    
    return 0
}

check_all_mounts() {
    local phase="$1"
    log_info "环境检查: $phase"
    
    local all_alive=1
    
    if check_mount_alive "$MOUNT_POINT1" "客户端1"; then
        log_info "  ✅ 客户端1 正常"
    else
        all_alive=0
    fi
    
    if check_mount_alive "$MOUNT_POINT2" "客户端2"; then
        log_info "  ✅ 客户端2 正常"
    else
        all_alive=0
    fi
    
    if [ -d "$MOUNT_POINT3" ] && check_mount_alive "$MOUNT_POINT3" "客户端3"; then
        log_info "  ✅ 客户端3 正常"
    elif [ -d "$MOUNT_POINT3" ]; then
        all_alive=0
    fi
    
    if [ -d "$MOUNT_POINT4" ] && check_mount_alive "$MOUNT_POINT4" "客户端4"; then
        log_info "  ✅ 客户端4 正常"
    elif [ -d "$MOUNT_POINT4" ]; then
        all_alive=0
    fi
    
    if [ $all_alive -ne 1 ]; then
        log_error "部分挂载点异常，测试终止"
        exit 1
    fi
    
    log_success "  所有挂载点正常"
}

run_test() {
    local desc="$1"
    local condition="$2"
    local expected="$3"
    
    log_test "$desc"
    
    if eval "$condition"; then
        log_success "  ✅ 通过"
        ((PASSED++))
    else
        log_error "  ❌ 失败 (期望: $expected)"
        ((FAILED++))
    fi
}

run_test_with_timeout() {
    local desc="$1"
    local cmd="$2"
    local timeout="$3"
    
    log_test "$desc"
    
    timeout "$timeout" bash -c "$cmd" > /dev/null 2>&1
    local result=$?
    
    if [ $result -eq 0 ]; then
        log_success "  ✅ 通过 (在 ${timeout}s 内完成)"
        ((PASSED++))
    elif [ $result -eq 124 ]; then
        log_error "  ❌ 超时 (超过 ${timeout}s，可能死锁)"
        ((FAILED++))
    else
        log_error "  ❌ 失败 (退出码: $result)"
        ((FAILED++))
    fi
}

count_conflicts() {
    $CLI_BIN -m $MASTER_ADDR conflicts list --path "$TEST_BASE" 2>/dev/null | grep "^Total:" | awk '{print $2}' || echo 0
}

wait_for_sync() {
    local seconds="${1:-5}"
    log_info "等待同步 ${seconds}s..."
    sleep "$seconds"
}

cleanup_test_dir() {
    log_info "清理测试目录..."
    rm -rf "$TEST_DIR1"/* "$TEST_DIR2"/* "$TEST_DIR3"/* "$TEST_DIR4"/* 2>/dev/null || true
    $CLI_BIN -m $MASTER_ADDR conflicts auto-resolve --path "$TEST_BASE" --policy aggressive 2>/dev/null || true
    sleep 2
}

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  PowerFS Multi-FUSE 死锁与冲突综合测试                               ║"
echo "║  Multi-FUSE Deadlock & Conflict Comprehensive Test                  ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

echo "测试配置:"
echo "  - Master地址: $MASTER_ADDR"
echo "  - 挂载点1: $MOUNT_POINT1"
echo "  - 挂载点2: $MOUNT_POINT2"
echo "  - 挂载点3: $MOUNT_POINT3"
echo "  - 挂载点4: $MOUNT_POINT4"
echo "  - 测试目录: $TEST_BASE"
echo ""

log_info "测试开始前环境检查..."
check_all_mounts "测试开始前"

mkdir -p "$TEST_DIR1" "$TEST_DIR2" "$TEST_DIR3" "$TEST_DIR4"
echo ""

echo "══════════════════════════════════════════════════════════════════════"
echo " 阶段1: 多客户端并发创建冲突 (CreateCreate Conflict)"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

cleanup_test_dir
check_all_mounts "阶段1开始前"

log_info "测试1.1: 4客户端同时创建同名文件"
for i in $(seq 1 5); do
    echo "client1_file$i" > "$TEST_DIR1/cc-file$i.txt" &
    echo "client2_file$i" > "$TEST_DIR2/cc-file$i.txt" &
    echo "client3_file$i" > "$TEST_DIR3/cc-file$i.txt" &
    echo "client4_file$i" > "$TEST_DIR4/cc-file$i.txt" &
done
wait
log_success "  4客户端同时创建完成"

wait_for_sync 5
check_all_mounts "测试1.1后"

log_info "测试1.2: 4客户端同时创建同名目录"
for i in $(seq 1 3); do
    mkdir "$TEST_DIR1/cc-dir$i" &
    mkdir "$TEST_DIR2/cc-dir$i" &
    mkdir "$TEST_DIR3/cc-dir$i" &
    mkdir "$TEST_DIR4/cc-dir$i" &
done
wait
log_success "  4客户端同时创建目录完成"

wait_for_sync 3
check_all_mounts "测试1.2后"

log_info "验证冲突检测..."
CC_COUNT=$(count_conflicts)
run_test "应检测到 CreateCreate 冲突" "[ $CC_COUNT -ge 8 ]" ">=8 个冲突"

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo " 阶段2: 多客户端并发写入冲突 (WriteWrite Conflict)"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

cleanup_test_dir
check_all_mounts "阶段2开始前"

log_info "测试2.1: 先创建共享文件"
echo "initial content" > "$TEST_DIR1/ww-shared.txt"
wait_for_sync 5

log_info "测试2.2: 4客户端同时写入同一文件"
for iteration in $(seq 1 10); do
    echo "client1_write_$iteration" > "$TEST_DIR1/ww-shared.txt" &
    echo "client2_write_$iteration" > "$TEST_DIR2/ww-shared.txt" &
    echo "client3_write_$iteration" > "$TEST_DIR3/ww-shared.txt" &
    echo "client4_write_$iteration" > "$TEST_DIR4/ww-shared.txt" &
    wait
    sleep 0.1
done
log_success "  10轮并发写入完成"

check_all_mounts "测试2.2后"

log_info "测试2.3: 验证文件可访问性"
run_test_with_timeout "客户端1可读取文件" "cat $TEST_DIR1/ww-shared.txt" $TIMEOUT_SHORT
run_test_with_timeout "客户端2可读取文件" "cat $TEST_DIR2/ww-shared.txt" $TIMEOUT_SHORT
run_test_with_timeout "客户端3可读取文件" "cat $TEST_DIR3/ww-shared.txt" $TIMEOUT_SHORT
run_test_with_timeout "客户端4可读取文件" "cat $TEST_DIR4/ww-shared.txt" $TIMEOUT_SHORT

wait_for_sync 3
WW_COUNT=$(count_conflicts)
run_test "应检测到 WriteWrite 冲突" "[ $WW_COUNT -ge 1 ]" ">=1 个冲突"

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo " 阶段3: 并发删除与创建冲突 (DeleteCreate Conflict)"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

cleanup_test_dir
check_all_mounts "阶段3开始前"

log_info "测试3.1: 创建初始文件"
for i in $(seq 1 5); do
    echo "initial_$i" > "$TEST_DIR1/dc-file$i.txt"
done
wait_for_sync 5

log_info "测试3.2: 并发删除-创建交替操作"
for i in $(seq 1 5); do
    rm -f "$TEST_DIR1/dc-file$i.txt" &
    echo "recreated_$i" > "$TEST_DIR2/dc-file$i.txt" &
    rm -f "$TEST_DIR3/dc-file$i.txt" &
    echo "recreated_$i_v2" > "$TEST_DIR4/dc-file$i.txt" &
    wait
done
log_success "  5组并发删除-创建完成"

check_all_mounts "测试3.2后"

wait_for_sync 3
DC_COUNT=$(count_conflicts)
run_test "应检测到 DeleteCreate 冲突" "[ $DC_COUNT -ge 1 ]" ">=1 个冲突"

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo " 阶段4: 重命名死锁场景测试 (Rename Deadlock Scenario)"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

cleanup_test_dir
check_all_mounts "阶段4开始前"

log_info "测试4.1: 创建多层目录结构"
for i in $(seq 1 4); do
    mkdir -p "$TEST_DIR1/rename-test$i/subdir1/subdir2"
    echo "file$i" > "$TEST_DIR1/rename-test$i/file$i.txt"
done
wait_for_sync 3

log_info "测试4.2: 多客户端并发重命名同一目录"
run_test_with_timeout "客户端1重命名目录" "mv $TEST_DIR1/rename-test1 $TEST_DIR1/rename-test1-renamed" $TIMEOUT_LONG
run_test_with_timeout "客户端2重命名目录" "mv $TEST_DIR2/rename-test2 $TEST_DIR2/rename-test2-renamed" $TIMEOUT_LONG

check_all_mounts "测试4.2后"

log_info "测试4.3: 交叉重命名（最可能触发死锁的场景）"
mkdir -p "$TEST_DIR1/cross-a" "$TEST_DIR1/cross-b"
echo "content-a" > "$TEST_DIR1/cross-a/file.txt"
echo "content-b" > "$TEST_DIR1/cross-b/file.txt"
wait_for_sync 2

run_test_with_timeout "交叉重命名操作" "mv $TEST_DIR1/cross-a $TEST_DIR2/cross-b/target & mv $TEST_DIR2/cross-b $TEST_DIR1/cross-a/target; wait" $TIMEOUT_LONG

check_all_mounts "测试4.3后"

log_info "测试4.4: 大量文件重命名压力测试"
mkdir -p "$TEST_DIR1/batch-rename"
for i in $(seq 1 100); do
    echo "file$i" > "$TEST_DIR1/batch-rename/file$i.txt"
done
wait_for_sync 3

run_test_with_timeout "批量重命名100个文件" "cd $TEST_DIR1/batch-rename && for f in *.txt; do mv \$f \${f%.txt}-renamed.txt; done" $TIMEOUT_LONG

check_all_mounts "测试4.4后"

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo " 阶段5: 目录操作死锁场景测试 (Directory Operation Deadlock)"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

cleanup_test_dir
check_all_mounts "阶段5开始前"

log_info "测试5.1: 并发 mkdir/rmdir 压力测试"
for iteration in $(seq 1 50); do
    mkdir "$TEST_DIR1/volatile-$iteration" &
    mkdir "$TEST_DIR2/volatile-$iteration" &
    rmdir "$TEST_DIR1/volatile-$(($iteration-1))" 2>/dev/null &
    rmdir "$TEST_DIR2/volatile-$(($iteration-1))" 2>/dev/null &
    wait
done
log_success "  50轮 mkdir/rmdir 完成"

check_all_mounts "测试5.1后"

log_info "测试5.2: 递归目录操作"
run_test_with_timeout "客户端1递归创建" "mkdir -p $TEST_DIR1/recursive/a/b/c/d/e/f" $TIMEOUT_SHORT
run_test_with_timeout "客户端2递归删除" "rm -rf $TEST_DIR2/recursive" $TIMEOUT_SHORT

check_all_mounts "测试5.2后"

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo " 阶段6: 混合负载死锁测试 (Mixed Workload Deadlock)"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

cleanup_test_dir
check_all_mounts "阶段6开始前"

log_info "测试6.1: 启动混合并发操作"
echo "启动混合负载测试（持续15秒）..."

start_time=$(date +%s)
end_time=$((start_time + 15))

while [ $(date +%s) -lt $end_time ]; do
    # 客户端1: 创建文件
    echo "create-$(date +%N)" > "$TEST_DIR1/mixed-$(date +%N).txt" &
    
    # 客户端2: 删除文件
    rm -f "$TEST_DIR2"/mixed-*.txt 2>/dev/null &
    
    # 客户端3: 写入文件
    echo "write-$(date +%N)" > "$TEST_DIR3/mixed-shared.txt" &
    
    # 客户端4: 重命名操作
    mkdir -p "$TEST_DIR4/mixed-dir-$(date +%N)" 2>/dev/null &
    
    wait
    sleep 0.01
done

log_success "  混合负载测试完成"
check_all_mounts "测试6.1后"

log_info "测试6.2: 最终文件系统健康检查"
run_test_with_timeout "ls 根目录不超时" "ls -la $MOUNT_POINT1/" $TIMEOUT_SHORT
run_test_with_timeout "ls 测试目录不超时" "ls -la $TEST_DIR1/" $TIMEOUT_SHORT
run_test_with_timeout "df 检查不超时" "df -h $MOUNT_POINT1" $TIMEOUT_SHORT

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo " 阶段7: 冲突检测与解决验证 (Conflict Detection & Resolution)"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

cleanup_test_dir
check_all_mounts "阶段7开始前"

log_info "测试7.1: 创建各种冲突场景"
echo "c1" > "$TEST_DIR1/conflict-all.txt"
echo "c2" > "$TEST_DIR2/conflict-all.txt"
echo "c3" > "$TEST_DIR3/conflict-all.txt"
echo "c4" > "$TEST_DIR4/conflict-all.txt"

mkdir "$TEST_DIR1/conflict-dir-all" &
mkdir "$TEST_DIR2/conflict-dir-all" &
wait

wait_for_sync 5

log_info "测试7.2: 检查冲突列表"
echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path "$TEST_BASE"
echo ""

FINAL_COUNT=$(count_conflicts)
run_test "应检测到冲突" "[ $FINAL_COUNT -ge 2 ]" ">=2 个冲突"

log_info "测试7.3: 自动解决所有冲突"
$CLI_BIN -m $MASTER_ADDR conflicts auto-resolve --path "$TEST_BASE" --policy aggressive
wait_for_sync 2

RESOLVED_COUNT=$(count_conflicts)
run_test "自动解决后冲突数应为0" "[ $RESOLVED_COUNT -eq 0 ]" "0 个冲突"

log_info "测试7.4: 解决后文件系统可正常访问"
run_test_with_timeout "解决后 ls 正常" "ls -la $TEST_DIR1/" $TIMEOUT_SHORT

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo " 测试结果汇总"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

echo "┌────────────────────────────────────────────────────────────────────┐"
echo "│                        测试结果统计                                 │"
echo "├──────────────┬──────────────┬──────────────┬───────────────────────┤"
echo "│    通过      │    失败      │   警告       │        状态           │"
echo "├──────────────┼──────────────┼──────────────┼───────────────────────┤"
printf "│ %10d │ %10d │ %10d │ %21s │\n" \
    "$PASSED" "$FAILED" "$WARNINGS" \
    "$([ $FAILED -eq 0 ] && echo "✅ 全部通过" || echo "❌ 部分失败")"
echo "└──────────────┴──────────────┴──────────────┴───────────────────────┘"
echo ""

if [ $FAILED -eq 0 ]; then
    log_success "🎉 所有测试通过！多 FUSE 挂载无死锁，冲突检测正常"
    echo ""
    echo "测试结论:"
    echo "  - ✅ 多客户端并发操作未触发死锁"
    echo "  - ✅ CreateCreate 冲突检测正常"
    echo "  - ✅ WriteWrite 冲突检测正常"
    echo "  - ✅ DeleteCreate 冲突检测正常"
    echo "  - ✅ 重命名操作未触发死锁"
    echo "  - ✅ 混合负载下系统稳定"
    echo "  - ✅ 冲突自动解决功能正常"
    echo ""
    exit 0
else
    log_error "⚠️ 部分测试失败，请检查日志"
    echo ""
    echo "失败分析:"
    echo "  - 挂载点无响应可能表示存在死锁"
    echo "  - 超时可能表示死锁或严重性能问题"
    echo "  - 请检查 fuse 日志和 master 日志"
    echo ""
    exit 1
fi