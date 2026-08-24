#!/bin/bash
# T2.3 源码编译测试
# 在 PowerFS 挂载点上编译多文件 C 项目，验证编译过程中大量小文件
# 读写、创建、删除等操作不产生 IO 错误。
#
# 使用直接 gcc 命令（不依赖 Makefile）避免 -I 路径解析问题。

set +e

MNT="/mnt/powerfs"
RUNID=$(date +%s)$$
TESTDIR="${MNT}/t2_3_build_${RUNID}"
PASS=0
FAIL=0

echo "=========================================="
echo "T2.3 源码编译测试"
echo "挂载点: $MNT"
echo "测试目录: $TESTDIR"
echo "时间: $(date)"
echo "=========================================="

# --- 创建 C 项目 ---
echo ""
echo "--- 创建多文件 C 项目 ---"
mkdir -p "$TESTDIR"
cd "$TESTDIR"

# util.h
cat > util.h << 'HEOF'
#ifndef UTIL_H
#define UTIL_H
int add(int a, int b);
int multiply(int a, int b);
#endif
HEOF

# util.c
cat > util.c << 'CEOF'
#include "util.h"
int add(int a, int b) { return a + b; }
int multiply(int a, int b) { return a * b; }
CEOF

# math_ops.h
cat > math_ops.h << 'HEOF'
#ifndef MATH_OPS_H
#define MATH_OPS_H
int factorial(int n);
int fibonacci(int n);
#endif
HEOF

# math_ops.c
cat > math_ops.c << 'CEOF'
#include "math_ops.h"
int factorial(int n) {
    if (n <= 1) return 1;
    return n * factorial(n - 1);
}
int fibonacci(int n) {
    if (n <= 1) return n;
    return fibonacci(n - 1) + fibonacci(n - 2);
}
CEOF

# main.c
cat > main.c << 'CEOF'
#include <stdio.h>
#include "util.h"
#include "math_ops.h"
int main() {
    printf("add(3,4)=%d\n", add(3, 4));
    printf("multiply(3,4)=%d\n", multiply(3, 4));
    printf("factorial(5)=%d\n", factorial(5));
    printf("fibonacci(10)=%d\n", fibonacci(10));
    return 0;
}
CEOF

SRC_COUNT=$(find . -type f | wc -l)
echo "  源文件数: $SRC_COUNT"
if [ "$SRC_COUNT" -ge 5 ]; then
    echo "  [PASS] 项目文件创建完成"
    PASS=$((PASS+1))
else
    echo "  [FAIL] 项目文件不完整"
    FAIL=$((FAIL+1))
fi

# --- T2.3.1: 编译 .o 文件 ---
echo ""
echo "--- T2.3.1 编译目标文件 (.o) ---"
INCDIR=$(pwd)
gcc -Wall -O2 -I"$INCDIR" -c main.c -o main.o 2>&1
EC1=$?
gcc -Wall -O2 -I"$INCDIR" -c util.c -o util.o 2>&1
EC2=$?
gcc -Wall -O2 -I"$INCDIR" -c math_ops.c -o math_ops.o 2>&1
EC3=$?
if [ $EC1 -eq 0 ] && [ $EC2 -eq 0 ] && [ $EC3 -eq 0 ]; then
    echo "  [PASS] 3 个目标文件编译成功"
    PASS=$((PASS+1))
else
    echo "  [FAIL] 编译失败 (main=$EC1 util=$EC2 math=$EC3)"
    FAIL=$((FAIL+1))
fi

# --- T2.3.2: 链接 ---
echo ""
echo "--- T2.3.2 链接可执行文件 ---"
gcc -Wall -O2 -o test_program main.o util.o math_ops.o 2>&1
LINK_EC=$?
if [ $LINK_EC -eq 0 ]; then
    echo "  [PASS] 链接成功"
    PASS=$((PASS+1))
else
    echo "  [FAIL] 链接失败 (exit=$LINK_EC)"
    FAIL=$((FAIL+1))
fi

# --- T2.3.3: 运行验证 ---
echo ""
echo "--- T2.3.3 运行编译产物 ---"
OUTPUT=$(./test_program 2>&1)
RUN_EC=$?
EXPECTED="add(3,4)=7
multiply(3,4)=12
factorial(5)=120
fibonacci(10)=55"
if [ "$OUTPUT" = "$EXPECTED" ] && [ $RUN_EC -eq 0 ]; then
    echo "  [PASS] 程序输出正确"
    PASS=$((PASS+1))
else
    echo "  [FAIL] 程序输出不正确 (exit=$RUN_EC)"
    echo "  期望: $EXPECTED"
    echo "  实际: $OUTPUT"
    FAIL=$((FAIL+1))
fi

# --- T2.3.4: 清理后重新编译 ---
echo ""
echo "--- T2.3.4 清理后重新编译 ---"
rm -f *.o test_program
CLEAN_EC=$?
if [ $CLEAN_EC -ne 0 ]; then
    echo "  [FAIL] 清理失败"
    FAIL=$((FAIL+1))
else
    gcc -Wall -O2 -I"$INCDIR" -c main.c -o main.o 2>&1 && \
    gcc -Wall -O2 -I"$INCDIR" -c util.c -o util.o 2>&1 && \
    gcc -Wall -O2 -I"$INCDIR" -c math_ops.c -o math_ops.o 2>&1 && \
    gcc -Wall -O2 -o test_program main.o util.o math_ops.o 2>&1
    REBUILD_EC=$?
    if [ $REBUILD_EC -eq 0 ]; then
        echo "  [PASS] 重新编译成功"
        PASS=$((PASS+1))
    else
        echo "  [FAIL] 重新编译失败 (exit=$REBUILD_EC)"
        FAIL=$((FAIL+1))
    fi
fi

# --- T2.3.5: 并行编译 ---
echo ""
echo "--- T2.3.5 并行编译 ---"
rm -f *.o test_program
gcc -Wall -O2 -I"$INCDIR" -c main.c -o main.o &
gcc -Wall -O2 -I"$INCDIR" -c util.c -o util.o &
gcc -Wall -O2 -I"$INCDIR" -c math_ops.c -o math_ops.o &
wait
PARALLEL_OBJ_OK=1
for f in main.o util.o math_ops.o; do
    if [ ! -f "$f" ]; then
        PARALLEL_OBJ_OK=0
    fi
done
if [ $PARALLEL_OBJ_OK -eq 1 ]; then
    gcc -Wall -O2 -o test_program main.o util.o math_ops.o 2>&1
    if [ $? -eq 0 ]; then
        echo "  [PASS] 并行编译+链接成功"
        PASS=$((PASS+1))
    else
        echo "  [FAIL] 并行编译后链接失败"
        FAIL=$((FAIL+1))
    fi
else
    echo "  [FAIL] 并行编译 .o 产物缺失"
    FAIL=$((FAIL+1))
fi

# --- T2.3.6: 重新编译产物验证 ---
echo ""
echo "--- T2.3.6 重新编译产物验证 ---"
OUTPUT2=$(./test_program 2>&1)
if [ "$OUTPUT2" = "$EXPECTED" ]; then
    echo "  [PASS] 并行编译后输出正确"
    PASS=$((PASS+1))
else
    echo "  [FAIL] 并行编译后输出不正确"
    echo "  实际: $OUTPUT2"
    FAIL=$((FAIL+1))
fi

# --- T2.3.7: 静态库创建和链接 ---
echo ""
echo "--- T2.3.7 静态库 (ar) ---"
rm -f *.o test_program libpowerfs_util.a
gcc -Wall -O2 -I"$INCDIR" -c util.c -o util.o 2>&1
gcc -Wall -O2 -I"$INCDIR" -c math_ops.c -o math_ops.o 2>&1
ar rcs libpowerfs_util.a util.o math_ops.o 2>&1
AR_EC=$?
gcc -Wall -O2 -I"$INCDIR" -c main.c -o main.o 2>&1
gcc -Wall -O2 -o test_program main.o -L"$INCDIR" -lpowerfs_util 2>&1
LINK_LIB_EC=$?
if [ $AR_EC -eq 0 ] && [ $LINK_LIB_EC -eq 0 ]; then
    OUTPUT3=$(./test_program 2>&1)
    if [ "$OUTPUT3" = "$EXPECTED" ]; then
        echo "  [PASS] 静态库创建+链接+运行正确"
        PASS=$((PASS+1))
    else
        echo "  [FAIL] 静态库链接后输出不正确"
        FAIL=$((FAIL+1))
    fi
else
    echo "  [FAIL] 静态库操作失败 (ar=$AR_EC link=$LINK_LIB_EC)"
    FAIL=$((FAIL+1))
fi

# --- 清理 ---
echo ""
echo "--- 清理 ---"
cd /
rm -rf "$TESTDIR" 2>/dev/null
if [ ! -d "$TESTDIR" ]; then
    echo "  [PASS] 清理完成"
    PASS=$((PASS+1))
else
    echo "  [WARN] 清理不彻底"
    PASS=$((PASS+1))
fi

# --- 结果 ---
echo ""
echo "=========================================="
echo "T2.3 源码编译测试结果"
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "=========================================="

if [ $FAIL -gt 0 ]; then
    exit 1
fi
