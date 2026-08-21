#!/usr/bin/env bash
# ================================================================
# T1: VFS 基础操作 — 严格正确性检查
#
# 覆盖：T1.1 文件CRUD / T1.2 目录操作 / T1.3 权限 / T1.5 边界
# 每条操作不仅验证 exit code，还断言 stat 字段 + MD5 内容一致性 + 跨客户端可见性。
#
# 运行: bash tests/functional/t1_vfs_basic.sh
# ================================================================
set -u
cd "$(dirname "$0")/../.."
source tests/lib/assertions.sh

CONTAINER="fuse-1"
TSTAMP=$(date +%s)
TDIR="/mnt/powerfs/func_${TSTAMP}_t1"

# ---- Preflight ----
preflight

# ---- Setup ----
section "T1: VFS Basic Operations"
docker exec "$CONTAINER" mkdir -p "$TDIR"
trap 'docker exec "$CONTAINER" rm -rf "$TDIR" 2>/dev/null' EXIT

# ================================================================
# T1.1 文件 CRUD
# ================================================================
echo ""
echo "  === T1.1 File CRUD ==="

# ---- T1.1a: create empty file ----
echo "  [T1.1a] create empty file"
f="$TDIR/empty.txt"
assert_ok "touch creates file" docker exec "$CONTAINER" touch "$f"
assert_exists "file exists" "$f"
# busybox stat reports "regular empty file" for 0-byte, "regular file" for non-empty
assert_stat "empty file: size=0 mode=644" "$f" '%s %a' "0 644"
ftype=$(docker exec "$CONTAINER" stat -c '%F' "$f" 2>/dev/null | tr -d '\r')
if [[ "$ftype" == "regular file" || "$ftype" == "regular empty file" ]]; then
    pass "empty file type is regular ($ftype)"
else
    fail "empty file type" "regular file" "$ftype"
fi

# ---- T1.1b: write 100B + MD5 verify ----
echo "  [T1.1b] write 100B + MD5 verify"
f="$TDIR/write100.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=100 count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "100B file size" "$f" '%s' "100"
assert_md5_match "100B content MD5" "$f" "$src_md5"

# ---- T1.1c: write 4KB + MD5 verify ----
echo "  [T1.1c] write 4KB + MD5 verify"
f="$TDIR/write4k.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=4096 count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "4KB file size" "$f" '%s' "4096"
assert_md5_match "4KB content MD5" "$f" "$src_md5"

# ---- T1.1d: write 1MB + MD5 verify ----
echo "  [T1.1d] write 1MB + MD5 verify"
f="$TDIR/write1m.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=1M count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "1MB file size" "$f" '%s' "1048576"
assert_md5_match "1MB content MD5" "$f" "$src_md5"

# ---- T1.1e: overwrite + old data gone ----
echo "  [T1.1e] overwrite + old data gone"
f="$TDIR/overwrite.txt"
docker exec "$CONTAINER" sh -c "echo old_data > '$f'"
old_md5=$(docker exec "$CONTAINER" md5sum "$f" 2>/dev/null | awk '{print $1}')
docker exec "$CONTAINER" sh -c "echo new > '$f'"
new_md5=$(docker exec "$CONTAINER" md5sum "$f" 2>/dev/null | awk '{print $1}')
assert_stat "overwrite size" "$f" '%s' "4"
if [[ "$old_md5" != "$new_md5" ]]; then
    pass "overwrite: MD5 changed after overwrite"
else
    fail "overwrite: MD5 unchanged" "different" "same=$old_md5"
fi

# ---- T1.1f: append + content = old + new ----
echo "  [T1.1f] append + content = old + new"
f="$TDIR/append.txt"
docker exec "$CONTAINER" sh -c "echo line1 > '$f'"
docker exec "$CONTAINER" sh -c "echo line2 >> '$f'"
assert_stat "append size" "$f" '%s' "12"
content=$(docker exec "$CONTAINER" cat "$f" 2>/dev/null | tr -d '\r')
expected=$'line1\nline2'
if [[ "$content" == "$expected" ]]; then
    pass "append: content = line1 + line2"
else
    fail "append: content" "$expected" "$content"
fi

# ---- T1.1g: truncate shrink ----
echo "  [T1.1g] truncate shrink"
f="$TDIR/trunc_shrink.bin"
docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=100 count=1 of='$f' 2>/dev/null"
docker exec "$CONTAINER" truncate -s 50 "$f"
assert_stat "truncate shrink size" "$f" '%s' "50"

# ---- T1.1h: truncate extend (zero fill) ----
echo "  [T1.1h] truncate extend (zero fill)"
f="$TDIR/trunc_extend.bin"
docker exec "$CONTAINER" sh -c "echo ab > '$f'"
docker exec "$CONTAINER" truncate -s 10 "$f"
assert_stat "truncate extend size" "$f" '%s' "10"
# 扩展部分应为 \0
tail_bytes=$(docker exec "$CONTAINER" sh -c "tail -c 7 '$f' | od -A n -t x1 | tr -d ' \n'")
if [[ "$tail_bytes" == "00000000000000" ]]; then
    pass "truncate extend: zero-filled tail"
else
    fail "truncate extend: zero fill" "00000000000000" "$tail_bytes"
fi

# ---- T1.1i: cross-client MD5 (防本地缓存假象) ----
echo "  [T1.1i] cross-client MD5 consistency"
f="$TDIR/cross_md5.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=512 count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
drop_cache fuse-2
assert_md5_cross "fuse-1→fuse-2 MD5 match" "$f" "fuse-1" "fuse-2"

# ================================================================
# T1.2 目录操作
# ================================================================
echo ""
echo "  === T1.2 Directory Operations ==="

# ---- T1.2a: mkdir + stat ----
echo "  [T1.2a] mkdir + stat"
d="$TDIR/subdir"
assert_ok "mkdir creates dir" docker exec "$CONTAINER" mkdir "$d"
assert_stat "mkdir: mode=755 type=directory" "$d" '%a %F' "755 directory"

# ---- T1.2b: mkdir -p nested ----
echo "  [T1.2b] mkdir -p nested"
d="$TDIR/nest/a/b/c"
assert_ok "mkdir -p creates nested" docker exec "$CONTAINER" mkdir -p "$d"
assert_exists "nested deepest dir exists" "$d"
assert_exists "nested intermediate exists" "$TDIR/nest/a/b"

# ---- T1.2c: rmdir empty ----
echo "  [T1.2c] rmdir empty dir"
d="$TDIR/rmdir_me"
docker exec "$CONTAINER" mkdir "$d"
assert_ok "rmdir empty succeeds" docker exec "$CONTAINER" rmdir "$d"
assert_not_exists "rmdir: dir gone" "$d"

# ---- T1.2d: rmdir non-empty fails ----
echo "  [T1.2d] rmdir non-empty fails"
d="$TDIR/nonempty"
docker exec "$CONTAINER" mkdir "$d"
docker exec "$CONTAINER" touch "$d/file"
assert_fail "rmdir non-empty fails" docker exec "$CONTAINER" rmdir "$d"

# ---- T1.2e: readdir (ls) ----
echo "  [T1.2e] readdir (ls)"
d="$TDIR/lsdir"
docker exec "$CONTAINER" mkdir "$d"
docker exec "$CONTAINER" touch "$d/alpha" "$d/beta" "$d/gamma"
entries=$(docker exec "$CONTAINER" ls -1 "$d" 2>/dev/null | sort | tr '\n' ' ')
expected="alpha beta gamma "
if [[ "$entries" == "$expected" ]]; then
    pass "readdir: entries match (alpha beta gamma)"
else
    fail "readdir: entries" "$expected" "$entries"
fi

# ---- T1.2f: rename file ----
echo "  [T1.2f] rename file"
old="$TDIR/rename_old.txt"
new="$TDIR/rename_new.txt"
docker exec "$CONTAINER" sh -c "echo rename_content > '$old'"
src_md5=$(docker exec "$CONTAINER" md5sum "$old" 2>/dev/null | awk '{print $1}')
assert_ok "rename file" docker exec "$CONTAINER" mv "$old" "$new"
assert_not_exists "rename: old path gone" "$old"
assert_exists "rename: new path exists" "$new"
assert_md5_match "rename: content preserved" "$new" "$src_md5"

# ---- T1.2g: rename directory ----
echo "  [T1.2g] rename directory"
old="$TDIR/renamedir_old"
new="$TDIR/renamedir_new"
docker exec "$CONTAINER" mkdir -p "$old/sub"
docker exec "$CONTAINER" touch "$old/file1" "$old/sub/file2"
assert_ok "rename dir" docker exec "$CONTAINER" mv "$old" "$new"
assert_not_exists "rename dir: old gone" "$old"
assert_exists "rename dir: new exists" "$new"
assert_exists "rename dir: sub/file2 exists" "$new/sub/file2"

# ---- T1.2h: unlink ----
echo "  [T1.2h] unlink"
f="$TDIR/unlink_me.txt"
docker exec "$CONTAINER" touch "$f"
assert_ok "unlink file" docker exec "$CONTAINER" rm "$f"
assert_not_exists "unlink: file gone" "$f"

# ---- T1.2i: symlink ----
echo "  [T1.2i] symlink"
target="$TDIR/sym_target.txt"
link="$TDIR/sym_link.txt"
docker exec "$CONTAINER" sh -c "echo target_data > '$target'"
assert_ok "create symlink" docker exec "$CONTAINER" ln -s "$target" "$link"
assert_stat "symlink type" "$link" '%F' "symbolic link"
link_target=$(docker exec "$CONTAINER" readlink "$link" 2>/dev/null | tr -d '\r\n')
if [[ "$link_target" == "$target" ]]; then
    pass "symlink: readlink correct ($link_target)"
else
    fail "symlink: readlink" "$target" "$link_target"
fi
# 通过 symlink 读内容
content=$(docker exec "$CONTAINER" cat "$link" 2>/dev/null | tr -d '\r\n')
if [[ "$content" == "target_data" ]]; then
    pass "symlink: read through link correct"
else
    fail "symlink: read through" "target_data" "$content"
fi

# ---- T1.2j: hardlink + nlink ----
echo "  [T1.2j] hardlink + nlink"
src="$TDIR/hard_src.txt"
hlink="$TDIR/hard_link.txt"
docker exec "$CONTAINER" sh -c "echo hardlink_data > '$src'"
assert_ok "create hardlink" docker exec "$CONTAINER" ln "$src" "$hlink"
assert_stat "hardlink: nlink=2 (src)" "$src" '%h' "2"
assert_stat "hardlink: nlink=2 (link)" "$hlink" '%h' "2"
# MD5 一致
src_md5=$(docker exec "$CONTAINER" md5sum "$src" 2>/dev/null | awk '{print $1}')
assert_md5_match "hardlink: content match" "$hlink" "$src_md5"
# 删除源后链接存活
docker exec "$CONTAINER" rm "$src"
assert_not_exists "hardlink: source deleted" "$src"
assert_exists "hardlink: link survives" "$hlink"
assert_stat "hardlink: nlink=1 after src delete" "$hlink" '%h' "1"

# ================================================================
# T1.3 权限测试
# ================================================================
echo ""
echo "  === T1.3 Permission Tests ==="

# ---- T1.3a: chmod ----
echo "  [T1.3a] chmod"
f="$TDIR/chmod_test.txt"
docker exec "$CONTAINER" touch "$f"
docker exec "$CONTAINER" chmod 600 "$f"
assert_stat "chmod 600" "$f" '%a' "600"
docker exec "$CONTAINER" chmod 755 "$f"
assert_stat "chmod 755" "$f" '%a' "755"
docker exec "$CONTAINER" chmod 700 "$f"
assert_stat "chmod 700" "$f" '%a' "700"

# ---- T1.3b: chown ----
echo "  [T1.3b] chown"
f="$TDIR/chown_test.txt"
docker exec "$CONTAINER" touch "$f"
docker exec "$CONTAINER" chown 1000:1000 "$f"
assert_stat "chown 1000:1000" "$f" '%u %g' "1000 1000"

# ---- T1.3c: cross-client chmod visibility ----
echo "  [T1.3c] cross-client chmod visibility"
f="$TDIR/cross_chmod.txt"
docker exec fuse-1 sh -c "echo x > '$f' && chmod 640 '$f'"
drop_cache fuse-2
mode_f2=$(docker exec fuse-2 stat -c '%a' "$f" 2>/dev/null | tr -d '\r\n')
if [[ "$mode_f2" == "640" ]]; then
    pass "cross-client chmod: fuse-2 sees mode=640"
else
    fail "cross-client chmod" "640" "$mode_f2"
fi

# ================================================================
# T1.5 边界测试
# ================================================================
echo ""
echo "  === T1.5 Boundary Tests ==="

# ---- T1.5a: empty filename (should fail) ----
echo "  [T1.5a] empty filename"
# Use openat with AT_EMPTY_PATH not available; test via touch of just ""
assert_fail "empty filename rejected" docker exec "$CONTAINER" sh -c "touch '$TDIR/'"

# ---- T1.5b: 255B filename (max allowed) ----
echo "  [T1.5b] 255B filename (max)"
long_name=$(printf 'a%.0s' {1..255})
f="$TDIR/$long_name"
assert_ok "255B filename creates" docker exec "$CONTAINER" touch "$f"
assert_exists "255B filename exists" "$f"

# ---- T1.5c: 256B filename (should fail) ----
echo "  [T1.5c] 256B filename (too long)"
too_long=$(printf 'b%.0s' {1..256})
f="$TDIR/$too_long"
assert_fail "256B filename rejected" docker exec "$CONTAINER" touch "$f"

# ---- T1.5d: spaces in filename ----
echo "  [T1.5d] spaces in filename"
f="$TDIR/file with spaces.txt"
assert_ok "space filename creates" docker exec "$CONTAINER" touch "$f"
assert_exists "space filename exists" "$f"

# ---- T1.5e: unicode filename ----
echo "  [T1.5e] unicode filename"
f="$TDIR/中文文件.txt"
assert_ok "unicode filename creates" docker exec "$CONTAINER" touch "$f"
assert_exists "unicode filename exists" "$f"

# ================================================================
# T1.4 特殊文件
# ================================================================
echo ""
echo "  === T1.4 Special Files ==="

# ---- T1.4a: FIFO (named pipe) ----
echo "  [T1.4a] mkfifo"
f="$TDIR/myfifo"
assert_ok "mkfifo creates pipe" docker exec "$CONTAINER" mkfifo "$f"
assert_exists "fifo exists" "$f"
ftype=$(docker exec "$CONTAINER" stat -c '%F' "$f" 2>/dev/null | tr -d '\r')
assert_eq "fifo type" "fifo" "$ftype"
# test -p should be true
assert_ok "fifo test -p" docker exec "$CONTAINER" test -p "$f"

# ---- T1.4b: symlink to directory ----
echo "  [T1.4b] symlink to directory"
d="$TDIR/symdir_target"
docker exec "$CONTAINER" mkdir -p "$d"
link="$TDIR/symdir_link"
assert_ok "create dir symlink" docker exec "$CONTAINER" ln -s "$d" "$link"
assert_stat "dir symlink type" "$link" '%F' "symbolic link"
# cd through symlink should work
assert_ok "cd through symlink" docker exec "$CONTAINER" sh -c "cd '$link' && pwd"

# ---- T1.4c: symlink to nonexistent target (dangling) ----
echo "  [T1.4c] dangling symlink"
link="$TDIR/dangling_link"
assert_ok "create dangling symlink" docker exec "$CONTAINER" ln -s "/nonexistent/path" "$link"
assert_exists "dangling symlink exists" "$link"
assert_stat "dangling symlink type" "$link" '%F' "symbolic link"
# stat (follow) should fail, lstat should succeed
assert_fail "stat dangling fails" docker exec "$CONTAINER" stat "$link"

# ================================================================
# T1.6 并发测试
# ================================================================
echo ""
echo "  === T1.6 Concurrent Access ==="

# ---- T1.6a: 4 processes write different files ----
echo "  [T1.6a] 4 parallel writers (different files)"
docker exec "$CONTAINER" mkdir -p "$TDIR/concurrent"
docker exec "$CONTAINER" sh -c "
for i in 1 2 3 4; do
    dd if=/dev/urandom bs=1024 count=1 2>/dev/null > '$TDIR/concurrent/writer_'\$i'.bin' &
done
wait
"
all_ok=true
for i in 1 2 3 4; do
    sz=$(docker exec "$CONTAINER" stat -c '%s' "$TDIR/concurrent/writer_${i}.bin" 2>/dev/null | tr -d '\r')
    if [[ "$sz" != "1024" ]]; then
        fail "concurrent writer $i size" "1024" "$sz"
        all_ok=false
    fi
done
if $all_ok; then
    pass "all 4 concurrent writers produced correct size (1024)"
fi
# Verify all 4 files have different MD5 (independent data)
md5_list=$(docker exec "$CONTAINER" sh -c "for i in 1 2 3 4; do md5sum '$TDIR/concurrent/writer_'\"\$i\"'.bin'; done" | awk '{print $1}' | sort -u | wc -l)
assert_eq "4 writers produce 4 distinct MD5s" "4" "$md5_list"

# ---- T1.6b: 4 processes read same file ----
echo "  [T1.6b] 4 parallel readers (same file)"
f="$TDIR/concurrent/shared_read.bin"
docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=4096 count=1 2>/dev/null > '$f'"
src_md5=$(docker exec "$CONTAINER" md5sum "$f" 2>/dev/null | awk '{print $1}')
docker exec "$CONTAINER" sh -c "
for i in 1 2 3 4; do
    md5sum '$f' > '$TDIR/concurrent/reader_'\$i'.md5' &
done
wait
"
all_match=true
for i in 1 2 3 4; do
    reader_md5=$(docker exec "$CONTAINER" awk '{print $1}' "$TDIR/concurrent/reader_${i}.md5" 2>/dev/null | tr -d '\r')
    if [[ "$reader_md5" != "$src_md5" ]]; then
        fail "concurrent reader $i MD5" "$src_md5" "$reader_md5"
        all_match=false
    fi
done
if $all_match; then
    pass "all 4 concurrent readers got identical MD5"
fi

# ================================================================
# T1.7 更多边界和语义检查
# ================================================================
echo ""
echo "  === T1.7 Extended Semantics ==="

# ---- T1.7a: . and .. in directory ----
echo "  [T1.7a] dot and dotdot entries"
d="$TDIR/dotdir"
docker exec "$CONTAINER" mkdir "$d"
assert_ok "cd . works" docker exec "$CONTAINER" sh -c "cd '$d' && pwd"
# .. should point to parent
parent_via_dotdot=$(docker exec "$CONTAINER" sh -c "cd '$d' && cd .. && pwd" | tr -d '\r')
parent_expected=$(docker exec "$CONTAINER" dirname "$d" | tr -d '\r')
assert_eq "cd .. points to parent" "$parent_expected" "$parent_via_dotdot"

# ---- T1.7b: mkdir existing fails ----
echo "  [T1.7b] mkdir existing dir fails"
d="$TDIR/existdir"
docker exec "$CONTAINER" mkdir "$d"
assert_fail "mkdir existing fails (EEXIST)" docker exec "$CONTAINER" mkdir "$d"

# ---- T1.7c: create file in nonexistent dir fails ----
echo "  [T1.7c] create in nonexistent dir fails"
assert_fail "create in nonexistent dir fails" docker exec "$CONTAINER" touch "$TDIR/no_such_dir/file.txt"

# ---- T1.7d: O_EXCL (exclusive create) ----
echo "  [T1.7d] O_EXCL exclusive create"
f="$TDIR/exclusive.txt"
docker exec "$CONTAINER" touch "$f"
# Opening with O_EXCL on existing file should fail
assert_fail "O_EXCL on existing fails" docker exec "$CONTAINER" sh -c "exec 3<> '$f' && python3 -c \"import os,fcntl; fcntl.open(3, os.O_EXCL)\" 2>/dev/null || true"

# ---- T1.7e: write at offset (sparse file) ----
echo "  [T1.7e] write at offset (sparse file)"
f="$TDIR/sparse.bin"
# Write 8 bytes at offset 1MB → file size should be 1MB+8
docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=8 count=1 seek=131072 of='$f' 2>/dev/null"
assert_stat "sparse file size" "$f" '%s' "1048584"

# ---- T1.7f: overwrite at specific offset ----
echo "  [T1.7f] overwrite at offset"
f="$TDIR/overwrite_offset.bin"
docker exec "$CONTAINER" sh -c "dd if=/dev/zero bs=100 count=1 of='$f' 2>/dev/null"
# Overwrite bytes 10-13 with 0xFF
docker exec "$CONTAINER" sh -c "printf '\xff\xff\xff\xff' | dd of='$f' bs=1 seek=10 conv=notrunc 2>/dev/null"
# Read bytes 10-13
hex_at_10=$(docker exec "$CONTAINER" sh -c "dd if='$f' bs=1 skip=10 count=4 2>/dev/null | od -A n -t x1 | tr -d ' \n'")
assert_eq "overwrite at offset 10-13" "ffffffff" "$hex_at_10"
# Bytes 0-9 should still be zero
hex_at_0=$(docker exec "$CONTAINER" sh -c "dd if='$f' bs=1 skip=0 count=10 2>/dev/null | od -A n -t x1 | tr -d ' \n'")
assert_eq "bytes 0-9 still zero" "00000000000000000000" "$hex_at_0"

# ---- T1.7g: deep nested directory (50 levels) ----
echo "  [T1.7g] deep nested directory (50 levels)"
deep="$TDIR/deep"
path="$deep"
for i in $(seq 1 50); do
    path="$path/d$i"
done
assert_ok "create 50-level deep dir" docker exec "$CONTAINER" mkdir -p "$path"
assert_exists "50-level deep dir exists" "$path"

# ---- T1.7h: many files in one directory (100) ----
echo "  [T1.7h] 100 files in one directory"
d="$TDIR/manyfiles"
docker exec "$CONTAINER" mkdir "$d"
docker exec "$CONTAINER" sh -c "for i in \$(seq 1 100); do touch '$d/file_'\$i; done"
count=$(docker exec "$CONTAINER" ls -1 "$d" 2>/dev/null | wc -l)
assert_eq "100 files created" "100" "$count"

# ---- T1.7i: stat on directory (nlink >= 2) ----
echo "  [T1.7i] directory nlink"
d="$TDIR/nlinkdir"
docker exec "$CONTAINER" mkdir "$d"
nlink=$(docker exec "$CONTAINER" stat -c '%h' "$d" 2>/dev/null | tr -d '\r')
# A directory with no subdirs should have nlink >= 2 (. and parent's entry)
if [[ "$nlink" -ge 2 ]]; then
    pass "empty dir nlink >= 2 (got $nlink)"
else
    fail "empty dir nlink >= 2" ">= 2" "$nlink"
fi
# Create a subdir → nlink should increase
docker exec "$CONTAINER" mkdir "$d/sub"
nlink2=$(docker exec "$CONTAINER" stat -c '%h' "$d" 2>/dev/null | tr -d '\r')
if [[ "$nlink2" -ge 3 ]]; then
    pass "dir with subdir nlink >= 3 (got $nlink2)"
else
    fail "dir with subdir nlink >= 3" ">= 3" "$nlink2"
fi

# ---- T1.7j: mtime update on write ----
echo "  [T1.7j] mtime update on write"
f="$TDIR/mtime_test.txt"
docker exec "$CONTAINER" touch "$f"
mtime_before=$(docker exec "$CONTAINER" stat -c '%Y' "$f" 2>/dev/null | tr -d '\r')
sleep 2
docker exec "$CONTAINER" sh -c "echo newdata > '$f'"
mtime_after=$(docker exec "$CONTAINER" stat -c '%Y' "$f" 2>/dev/null | tr -d '\r')
if [[ "$mtime_after" -gt "$mtime_before" ]]; then
    pass "mtime updated after write ($mtime_before → $mtime_after)"
else
    fail "mtime updated after write" "> $mtime_before" "$mtime_after"
fi

# ---- T1.7k: cross-client file creation visibility ----
echo "  [T1.7k] cross-client file creation visibility"
f="$TDIR/cross_create.txt"
docker exec fuse-1 sh -c "echo from_fuse1 > '$f'"
drop_cache fuse-2
content_f2=$(docker exec fuse-2 cat "$f" 2>/dev/null | tr -d '\r\n')
assert_eq "fuse-2 sees fuse-1 created file" "from_fuse1" "$content_f2"

# ---- T1.7l: cross-client file deletion visibility ----
echo "  [T1.7l] cross-client file deletion visibility"
f="$TDIR/cross_delete.txt"
docker exec fuse-1 sh -c "echo deletetest > '$f'"
drop_cache fuse-2
# Verify fuse-2 can see it
assert_exists "fuse-2 sees file before delete" "$f"
# Delete on fuse-1
docker exec fuse-1 rm "$f"
drop_cache fuse-2
assert_not_exists "fuse-2 sees file gone after delete" "$f"

# ================================================================
# 日志检查 + 汇总
# ================================================================
echo ""
section "Log Check"
check_logs_clean "T1" "fuse-1"
check_logs_clean "T1" "filer-1"

print_summary
