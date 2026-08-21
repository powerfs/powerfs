#!/bin/bash
# =============================================================================
# PowerFS L4 多客户端一致性测试 (在宿主机运行, 通过 docker exec 操作两个 FUSE 客户端)
#
# 测试架构:
#   Client A = fuse-1-test (172.30.0.40)
#   Client B = fuse-2-test (172.30.0.41)
#   两个客户端挂载同一 PowerFS 后端
#
# 测试项 (按 fs-test-plan.md):
#   L4.01-L4.10: 跨客户端数据可见性
#   L4.11-L4.16: 缓存协同 (Invalidate 机制)
#   L4.17-L4.22: Lease 仲裁与写一致性
#   L4.23-L4.28: 跨客户端元数据一致性
#   L4.29:       FUSE ↔ FUSE 组合
# =============================================================================

set -u
set +H          # 禁用历史扩展

PASS=0
FAIL=0
FAILED_TESTS=()

CLIENT_A="fuse-1-test"
CLIENT_B="fuse-2-test"
TEST_DIR="/mnt/fuse/l4_consistency_v2"

log()  { echo "[$(date '+%H:%M:%S')] $1"; }
ok()   { echo "  OK: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); FAILED_TESTS+=("$1"); }

assert_eq() {
    local expected="$1" actual="$2" name="$3"
    if [ "$expected" = "$actual" ]; then
        ok "$name"
    else
        fail "$name (expected=[$expected] actual=[$actual])"
    fi
}

# 在 Client A 执行
a() { docker exec "$CLIENT_A" bash -c "set +H; $1"; }

# 在 Client B 执行
b() { docker exec "$CLIENT_B" bash -c "set +H; $1"; }

# 在两个客户端同时执行 (后台)
par_b() { docker exec "$CLIENT_B" bash -c "set +H; $1" & }
par_a() { docker exec "$CLIENT_A" bash -c "set +H; $1" & }

echo "============================================================"
echo "  PowerFS L4 多客户端一致性测试"
echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Client A: $CLIENT_A"
echo "  Client B: $CLIENT_B"
echo "============================================================"
echo ""

# ── 准备 ──
log "准备测试环境..."
a "rm -rf $TEST_DIR && mkdir -p $TEST_DIR"
sleep 1

# 验证两个客户端都能访问测试目录
A_LS=$(a "ls $TEST_DIR 2>&1")
B_LS=$(b "ls $TEST_DIR 2>&1")
if [ -z "$A_LS" ] && [ -z "$B_LS" ]; then
    log "两个客户端均可访问 $TEST_DIR"
else
    log "警告: 客户端访问异常 A=[$A_LS] B=[$B_LS]"
fi

# ============================================================
echo "--- L4.01-L4.10: 跨客户端数据可见性 ---"
# ============================================================

# L4.01: 写后读可见性
log "L4.01: 写后读可见性"
a "echo 'hello from A' > $TEST_DIR/vis_write.txt"
sleep 0.5
B_READ=$(b "cat $TEST_DIR/vis_write.txt 2>/dev/null")
assert_eq "hello from A" "$B_READ" "L4.01 写后读可见性"

# L4.02: 覆盖写可见性
log "L4.02: 覆盖写可见性"
a "echo 'first' > $TEST_DIR/vis_overwrite.txt"
sleep 0.3
a "echo 'second' > $TEST_DIR/vis_overwrite.txt"
sleep 0.5
B_READ=$(b "cat $TEST_DIR/vis_overwrite.txt 2>/dev/null")
assert_eq "second" "$B_READ" "L4.02 覆盖写可见性"

# L4.03: 追加写可见性
log "L4.03: 追加写可见性"
a "echo 'line1' > $TEST_DIR/vis_append.txt"
sleep 0.3
a "echo 'line2' >> $TEST_DIR/vis_append.txt"
sleep 0.5
B_READ=$(b "cat $TEST_DIR/vis_append.txt 2>/dev/null")
assert_eq $'line1\nline2' "$B_READ" "L4.03 追加写可见性"

# L4.04: truncate 可见性
log "L4.04: truncate 可见性"
a "echo '1234567890' > $TEST_DIR/vis_trunc.txt"
sleep 0.3
a "truncate -s 5 $TEST_DIR/vis_trunc.txt"
sleep 0.5
B_SIZE=$(b "stat -c '%s' $TEST_DIR/vis_trunc.txt 2>/dev/null")
assert_eq "5" "$B_SIZE" "L4.04 truncate 可见性"

# L4.05: truncate 扩大 + 空洞
log "L4.05: truncate 扩大内容可见"
a "echo 'short' > $TEST_DIR/vis_trunc_ext.txt"
sleep 0.3
a "truncate -s 100 $TEST_DIR/vis_trunc_ext.txt"
sleep 0.5
B_SIZE=$(b "stat -c '%s' $TEST_DIR/vis_trunc_ext.txt 2>/dev/null")
B_CONTENT=$(b "cat $TEST_DIR/vis_trunc_ext.txt 2>/dev/null | head -c 5")
assert_eq "100" "$B_SIZE" "L4.05 truncate 扩大 size"
assert_eq "short" "$B_CONTENT" "L4.05 truncate 扩大内容保留"

# L4.06: rename 可见性
log "L4.06: rename 可见性"
a "echo 'rename test' > $TEST_DIR/Use existing I/O test tools, not hand‚Äëcrafted shell scripts.vis_rename_src.txt"
sleep 0.3
a "mv $TEST_DIR/vis_rename_src.txt $TEST_DIR/vis_rename_dst.txt"
sleep 0.5
B_SRC=$(b "test -f $TEST_DIR/vis_rename_src.txt && echo exists || echo gone")
B_DST=$(b "test -f $TEST_DIR/vis_rename_dst.txt && echo exists || echo gone")
assert_eq "gone" "$B_SRC" "L4.06 rename 源文件不可见"
assert_eq "exists" "$B_DST" "L4.06 rename 目标文件可见"

# L4.07: unlink 可见性
log "L4.07: unlink 可见性"
a "echo 'delete me' > $TEST_DIR/vis_unlink.txt"
sleep 0.3
a "rm $TEST_DIR/vis_unlink.txt"
sleep 0.5
B_EXISTS=$(b "test -f $TEST_DIR/vis_unlink.txt && echo exists || echo gone")
assert_eq "gone" "$B_EXISTS" "L4.07 unlink 可见性"

# L4.08: create 可见性
log "L4.08: create 可见性"
a "echo 'new file' > $TEST_DIR/vis_create.txt"
sleep 0.5
B_EXISTS=$(b "test -f $TEST_DIR/vis_create.txt && echo exists || echo gone")
assert_eq "exists" "$B_EXISTS" "L4.08 create 可见性"

# L4.09: mkdir 可见性
log "L4.09: mkdir 可见性"
a "mkdir -p $TEST_DIR/vis_mkdir"
sleep 0.5
B_EXISTS=$(b "test -d $TEST_DIR/vis_mkdir && echo exists || echo gone")
assert_eq "exists" "$B_EXISTS" "L4.09 mkdir 可见性"

# L4.10: rmdir 可见性
log "L4.10: rmdir 可见性"
a "mkdir -p $TEST_DIR/vis_rmdir && rmdir $TEST_DIR/vis_rmdir"
sleep 0.5
B_EXISTS=$(b "test -d $TEST_DIR/vis_rmdir && echo exists || echo gone")
assert_eq "gone" "$B_EXISTS" "L4.10 rmdir 可见性"

echo ""

# ============================================================
echo "--- L4.11-L4.16: 缓存协同 (Invalidate 机制) ---"
# ============================================================

# L4.11: 页缓存失效
log "L4.11: 页缓存失效"
a "echo 'original' > $TEST_DIR/cache_page.txt"
sleep 0.3
# B 读一次（缓存）
b "cat $TEST_DIR/cache_page.txt > /dev/null"
sleep 0.3
# A 修改
a "echo 'modified' > $TEST_DIR/cache_page.txt"
sleep 1
# B 再读
B_READ=$(b "cat $TEST_DIR/cache_page.txt 2>/dev/null")
assert_eq "modified" "$B_READ" "L4.11 页缓存失效"

# L4.12: 目录缓存失效
log "L4.12: 目录缓存失效"
a "mkdir -p $TEST_DIR/cache_dir"
sleep 0.3
# B ls 一次（缓存）
b "ls $TEST_DIR/cache_dir > /dev/null 2>&1"
sleep 0.3
# A 在目录中创建文件
a "echo 'new' > $TEST_DIR/cache_dir/newfile.txt"
sleep 1
# B 再 ls
B_LS=$(b "ls $TEST_DIR/cache_dir 2>/dev/null")
assert_eq "newfile.txt" "$B_LS" "L4.12 目录缓存失效"

# L4.13: dentry 缓存失效
log "L4.13: dentry 缓存失效"
a "echo 'dentry' > $TEST_DIR/cache_dentry.txt"
sleep 0.3
# B lookup（缓存）
b "stat $TEST_DIR/cache_dentry.txt > /dev/null 2>&1"
sleep 0.3
# A rename
a "mv $TEST_DIR/cache_dentry.txt $TEST_DIR/cache_dentry_renamed.txt"
sleep 2
# B 再 lookup 旧名 (需清除内核 dentry 缓存)
B_OLD=$(b "stat $TEST_DIR/cache_dentry.txt 2>/dev/null && echo found || echo notfound")
assert_eq "notfound" "$B_OLD" "L4.13 dentry 缓存失效"

# L4.14: getattr 缓存失效
log "L4.14: getattr 缓存失效"
a "echo 'attr' > $TEST_DIR/cache_getattr.txt"
sleep 0.3
# B stat（缓存）
b "stat -c '%a' $TEST_DIR/cache_getattr.txt > /dev/null 2>&1"
sleep 0.3
# A chmod
a "chmod 0600 $TEST_DIR/cache_getattr.txt"
sleep 1
# B 再 stat
B_MODE=$(b "stat -c '%a' $TEST_DIR/cache_getattr.txt 2>/dev/null")
assert_eq "600" "$B_MODE" "L4.14 getattr 缓存失效"

# L4.15: 大文件缓存失效
log "L4.15: 大文件缓存失效"
a "dd if=/dev/urandom of=$TEST_DIR/cache_big.bin bs=1M count=1 2>/dev/null"
A_MD5=$(a "md5sum $TEST_DIR/cache_big.bin" | awk '{print $1}')
sleep 0.3
# B 读（缓存）
b "md5sum $TEST_DIR/cache_big.bin > /dev/null 2>&1"
sleep 0.3
# A 覆盖
a "dd if=/dev/urandom of=$TEST_DIR/cache_big.bin bs=1M count=1 2>/dev/null"
A_MD5_NEW=$(a "md5sum $TEST_DIR/cache_big.bin" | awk '{print $1}')
sleep 2
# B 再读 (需清除内核页缓存)
b "echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || true"
B_MD5=$(b "md5sum $TEST_DIR/cache_big.bin 2>/dev/null" | awk '{print $1}')
if [ "$A_MD5" != "$A_MD5_NEW" ]; then
    assert_eq "$A_MD5_NEW" "$B_MD5" "L4.15 大文件缓存失效"
else
    fail "L4.15 大文件缓存失效 (A 覆盖后 md5 未变化, 数据相同)"
fi

# L4.16: Invalidate 延迟
log "L4.16: Invalidate 延迟"
a "echo 'delay test' > $TEST_DIR/cache_delay.txt"
sleep 0.3
b "cat $TEST_DIR/cache_delay.txt > /dev/null"
sleep 0.3
START=$(date +%s%N)
a "echo 'delayed' > $TEST_DIR/cache_delay.txt"
# B 轮询直到看到变化
for i in $(seq 1 30); do
    B_READ=$(b "cat $TEST_DIR/cache_delay.txt 2>/dev/null")
    if [ "$B_READ" = "delayed" ]; then
        END=$(date +%s%N)
        ELAPSE_MS=$(( (END - START) / 1000000 ))
        break
    fi
    sleep 0.1
    ELAPSE_MS=99999
done
if [ "$B_READ" = "delayed" ]; then
    echo "  OK: L4.16 Invalidate 延迟 (${ELAPSE_MS}ms)"
    PASS=$((PASS+1))
else
    fail "L4.16 Invalidate 延迟 (B 未看到变更, B_READ=[$B_READ])"
fi

echo ""

# ============================================================
echo "--- L4.17-L4.22: Lease 仲裁与写一致性 ---"
# ============================================================

# L4.17: 同文件并发写不同 offset
log "L4.17: 同文件并发写不同 offset"
# 用 dd 创建 2MB 文件 (不用 truncate, 避免某些 FUSE 实现不支持 truncate 扩展)
a "dd if=/dev/zero of=$TEST_DIR/lease_concurrent.bin bs=1M count=2 2>/dev/null"
sleep 0.5
# A 写 [0, 1MB), B 写 [1MB, 2MB) 同时
a "dd if=/dev/zero bs=1M count=1 conv=notrunc of=$TEST_DIR/lease_concurrent.bin 2>/dev/null" &
PID_A=$!
b "dd if=/dev/zero bs=1M count=1 conv=notrunc seek=1 of=$TEST_DIR/lease_concurrent.bin 2>/dev/null" &
PID_B=$!
wait $PID_A $PID_B 2>/dev/null
sleep 1
B_SIZE=$(b "stat -c '%s' $TEST_DIR/lease_concurrent.bin 2>/dev/null")
B_MD5=$(b "md5sum $TEST_DIR/lease_concurrent.bin 2>/dev/null" | awk '{print $1}')
A_MD5=$(a "md5sum $TEST_DIR/lease_concurrent.bin 2>/dev/null" | awk '{print $1}')
assert_eq "2097152" "$B_SIZE" "L4.17 并发写不同 offset size"
assert_eq "$A_MD5" "$B_MD5" "L4.17 并发写不同 offset 一致性"

# L4.18: 同文件并发写同 offset
log "L4.18: 同文件并发写同 offset"
a "echo 'initial' > $TEST_DIR/lease_same_offset.txt"
sleep 0.3
# A 和 B 同时写 offset 0
a "echo 'AAAA' > $TEST_DIR/lease_same_offset.txt" &
PID_A=$!
b "echo 'BBBB' > $TEST_DIR/lease_same_offset.txt" &
PID_B=$!
wait $PID_A $PID_B 2>/dev/null
sleep 1
# 后写覆盖先写 (任一结果均可, 关键是一致)
A_READ=$(a "cat $TEST_DIR/lease_same_offset.txt 2>/dev/null")
B_READ=$(b "cat $TEST_DIR/lease_same_offset.txt 2>/dev/null")
if [ "$A_READ" = "$B_READ" ] && { [ "$A_READ" = "AAAA" ] || [ "$A_READ" = "BBBB" ]; }; then
    ok "L4.18 并发写同 offset 一致 ($A_READ)"
else
    fail "L4.18 并发写同 offset 一致 (A=[$A_READ] B=[$B_READ])"
fi

# L4.19: Lease 排他性 (A 写大文件持 lease, B 写同文件需等待)
log "L4.19: Lease 排他性"
# A 写 4MB 文件 (持续写入约 2-3 秒, 持有写 lease)
a "dd if=/dev/zero of=$TEST_DIR/lease_excl.txt bs=4M count=1 2>/dev/null" &
PID_A=$!
sleep 0.5
# B 尝试写同一文件 (应等待 A 完成)
START=$(date +%s%N)
b "echo 'B writing' >> $TEST_DIR/lease_excl.txt"
END=$(date +%s%N)
ELAPSE_MS=$(( (END - START) / 1000000 ))
wait $PID_A 2>/dev/null
sleep 0.5
A_CONTENT=$(a "cat $TEST_DIR/lease_excl.txt 2>/dev/null | tail -1")
B_CONTENT=$(b "cat $TEST_DIR/lease_excl.txt 2>/dev/null | tail -1")
if [ "$A_CONTENT" = "$B_CONTENT" ]; then
    echo "  OK: L4.19 Lease 排他性 (B 等待 ${ELAPSE_MS}ms, 内容一致)"
    PASS=$((PASS+1))
else
    fail "L4.19 Lease 排他性 (B 等待 ${ELAPSE_MS}ms, A=[$A_CONTENT] B=[$B_CONTENT])"
fi

# L4.20: Lease 续约期间读 (A 持写 lease, B 读不阻塞)
log "L4.20: Lease 续约期间读"
a "echo 'lease read test' > $TEST_DIR/lease_read_during.txt"
sleep 0.3
# A 后台写大文件 (持 lease)
a "dd if=/dev/zero of=$TEST_DIR/lease_read_during.bin bs=4M count=1 2>/dev/null" &
PID_A=$!
sleep 0.5
# B 读另一个文件 (应不阻塞)
B_READ=$(b "cat $TEST_DIR/lease_read_during.txt 2>/dev/null")
wait $PID_A 2>/dev/null
assert_eq "lease read test" "$B_READ" "L4.20 Lease 续约期间读"

# L4.21: 并发 append 顺序 (正确转义 $ 防止外层 shell 展开)
log "L4.21: 并发 append 顺序"
a "rm -f $TEST_DIR/lease_append.txt; touch $TEST_DIR/lease_append.txt"
sleep 0.3
# 转义 \$(seq) 和 \${i} 使其由内层 bash 展开, $TEST_DIR 由外层展开
a "for i in \$(seq 1 100); do echo \"A_line_\${i}\" >> $TEST_DIR/lease_append.txt; done" &
PID_A=$!
b "for i in \$(seq 1 100); do echo \"B_line_\${i}\" >> $TEST_DIR/lease_append.txt; done" &
PID_B=$!
wait $PID_A $PID_B 2>/dev/null
sleep 1
A_LINES=$(a "wc -l < $TEST_DIR/lease_append.txt 2>/dev/null")
B_LINES=$(b "wc -l < $TEST_DIR/lease_append.txt 2>/dev/null")
A_COUNT_A=$(a "grep -c '^A_line_' $TEST_DIR/lease_append.txt 2>/dev/null")
A_COUNT_B=$(a "grep -c '^B_line_' $TEST_DIR/lease_append.txt 2>/dev/null")
assert_eq "200" "$A_LINES" "L4.21 并发 append 总行数 (A 视角)"
assert_eq "200" "$B_LINES" "L4.21 并发 append 总行数 (B 视角)"
assert_eq "100" "$A_COUNT_A" "L4.21 并发 append A 行数"
assert_eq "100" "$A_COUNT_B" "L4.21 并发 append B 行数"

# L4.22: close 释放 lease
log "L4.22: close 释放 lease"
a "echo 'close test' > $TEST_DIR/lease_close.txt"
sleep 0.3
# A open + close (获取并释放 lease)
a "exec 3>$TEST_DIR/lease_close.txt; echo 'A wrote'; exec 3>&-"
sleep 0.5
# B 写 (应立即获取 lease, 无等待)
START=$(date +%s%N)
b "echo 'B wrote' >> $TEST_DIR/lease_close.txt"
END=$(date +%s%N)
ELAPSE_MS=$(( (END - START) / 1000000 ))
B_CONTENT=$(b "cat $TEST_DIR/lease_close.txt 2>/dev/null")
if [ $ELAPSE_MS -lt 2000 ]; then
    echo "  OK: L4.22 close 释放 lease (B 等待 ${ELAPSE_MS}ms)"
    PASS=$((PASS+1))
else
    fail "L4.22 close 释放 lease (B 等待 ${ELAPSE_MS}ms, 可能未释放)"
fi

echo ""

# ============================================================
echo "--- L4.23-L4.28: 跨客户端元数据一致性 ---"
# ============================================================

# L4.23: chmod 跨客户端
log "L4.23: chmod 跨客户端"
a "echo 'chmod test' > $TEST_DIR/meta_chmod.txt"
sleep 0.3
a "chmod 0755 $TEST_DIR/meta_chmod.txt"
sleep 0.5
B_MODE=$(b "stat -c '%a' $TEST_DIR/meta_chmod.txt 2>/dev/null")
assert_eq "755" "$B_MODE" "L4.23 chmod 跨客户端"

# L4.24: chown 跨客户端
log "L4.24: chown 跨客户端"
a "echo 'chown test' > $TEST_DIR/meta_chown.txt"
sleep 0.3
a "chown 1000:1000 $TEST_DIR/meta_chown.txt" 2>/dev/null
sleep 0.5
B_UID=$(b "stat -c '%u' $TEST_DIR/meta_chown.txt 2>/dev/null")
B_GID=$(b "stat -c '%g' $TEST_DIR/meta_chown.txt 2>/dev/null")
assert_eq "1000" "$B_UID" "L4.24 chown uid 跨客户端"
assert_eq "1000" "$B_GID" "L4.24 chown gid 跨客户端"

# L4.25: utimes 跨客户端
log "L4.25: utimes 跨客户端"
a "echo 'utimes test' > $TEST_DIR/meta_utimes.txt"
sleep 0.3
a "touch -d '2020-01-01 00:00:00' $TEST_DIR/meta_utimes.txt"
sleep 0.5
B_MTIME=$(b "stat -c '%Y' $TEST_DIR/meta_utimes.txt 2>/dev/null")
assert_eq "1577836800" "$B_MTIME" "L4.25 utimes 跨客户端"

# L4.26: hardlink 跨客户端
log "L4.26: hardlink 跨客户端"
a "echo 'hardlink test' > $TEST_DIR/meta_hl_orig.txt"
sleep 0.3
a "ln $TEST_DIR/meta_hl_orig.txt $TEST_DIR/meta_hl_link.txt"
sleep 0.5
B_NLINK=$(b "stat -c '%h' $TEST_DIR/meta_hl_orig.txt 2>/dev/null")
assert_eq "2" "$B_NLINK" "L4.26 hardlink nlink 跨客户端"

# L4.27: hardlink 删除跨客户端
log "L4.27: hardlink 删除跨客户端"
a "rm $TEST_DIR/meta_hl_link.txt"
sleep 0.5
B_NLINK=$(b "stat -c '%h' $TEST_DIR/meta_hl_orig.txt 2>/dev/null")
assert_eq "1" "$B_NLINK" "L4.27 hardlink 删除 nlink 跨客户端"

# L4.28: symlink 跨客户端
log "L4.28: symlink 跨客户端"
a "echo 'symlink target' > $TEST_DIR/meta_sym_target.txt"
sleep 0.3
a "ln -s meta_sym_target.txt $TEST_DIR/meta_sym_link.txt"
sleep 0.5
B_TARGET=$(b "readlink $TEST_DIR/meta_sym_link.txt 2>/dev/null")
B_CONTENT=$(b "cat $TEST_DIR/meta_sym_link.txt 2>/dev/null")
assert_eq "meta_sym_target.txt" "$B_TARGET" "L4.28 symlink target 跨客户端"
assert_eq "symlink target" "$B_CONTENT" "L4.28 symlink 内容 跨客户端"

echo ""

# ============================================================
echo "--- L4.29: FUSE ↔ FUSE 组合 ---"
# ============================================================

# L4.29: FUSE↔FUSE 综合测试 (A 写 B 读 + B 写 A 读)
log "L4.29: FUSE ↔ FUSE 综合测试"
a "echo 'A to B' > $TEST_DIR/fuse_ab.txt"
sleep 0.5
B_READ=$(b "cat $TEST_DIR/fuse_ab.txt 2>/dev/null")
assert_eq "A to B" "$B_READ" "L4.29.01 A→B 数据可见"

b "echo 'B to A' > $TEST_DIR/fuse_ba.txt"
sleep 0.5
A_READ=$(a "cat $TEST_DIR/fuse_ba.txt 2>/dev/null")
assert_eq "B to A" "$A_READ" "L4.29.02 B→A 数据可见"

# A 和 B 交替写同一文件
a "echo 'A1' > $TEST_DIR/fuse_interleave.txt"
sleep 0.3
b "echo 'B1' >> $TEST_DIR/fuse_interleave.txt"
sleep 0.3
a "echo 'A2' >> $TEST_DIR/fuse_interleave.txt"
sleep 0.3
b "echo 'B2' >> $TEST_DIR/fuse_interleave.txt"
sleep 2
# A 读取前清除内核页缓存
a "echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || true"
A_READ=$(a "cat $TEST_DIR/fuse_interleave.txt 2>/dev/null")
B_READ=$(b "cat $TEST_DIR/fuse_interleave.txt 2>/dev/null")
EXPECTED=$'A1\nB1\nA2\nB2'
assert_eq "$EXPECTED" "$A_READ" "L4.29.03 交替写 A 视角"
assert_eq "$EXPECTED" "$B_READ" "L4.29.04 交替写 B 视角"

echo ""

# ── 清理 ──
log "清理测试数据..."
a "rm -rf $TEST_DIR" 2>/dev/null

# ── 汇总 ──
echo ""
echo "============================================================"
echo "  L4 多客户端一致性测试汇总"
echo "============================================================"
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  TOTAL: $((PASS+FAIL))"
echo ""
if [ $FAIL -gt 0 ]; then
    echo "  失败项:"
    for t in "${FAILED_TESTS[@]}"; do
        echo "    - $t"
    done
fi
echo "============================================================"
