#!/usr/bin/env bash
# ================================================================
# T8: 持久化测试 — remount 后数据完整性验证
#
# 覆盖：写入/删除/硬链接/软链接/truncate/元数据/rename 持久化 + manifest 对比
# 核心断言：remount 后 find+md5sum manifest 完全一致 + stat 字段不变
# 注意：remount 通过 `docker restart fuse-1` 实现，需确保卷映射正确
#
# 运行: bash tests/functional/t8_persistence.sh
# ================================================================
set -u
cd "$(dirname "$0")/../.."
source tests/lib/assertions.sh

CONTAINER="fuse-1"
TSTAMP=$(date +%s)
TDIR="/mnt/powerfs/func_${TSTAMP}_t8"

preflight

section "T8: Persistence Tests (remount verification)"
docker exec "$CONTAINER" mkdir -p "$TDIR"
# Note: T8 does NOT clean up TDIR — persistent data should survive remount

# Helper: restart fuse-1 and wait for mount
restart_fuse() {
    echo "  [restart] Restarting fuse-1..."
    docker restart fuse-1 >/dev/null 2>&1
    # Wait for mount to be ready
    local retries=0
    while ! docker exec fuse-1 test -d /mnt/powerfs 2>/dev/null; do
        retries=$((retries + 1))
        if [[ $retries -gt 30 ]]; then
            echo "  [restart] ERROR: fuse-1 mount not ready after 30s"
            return 1
        fi
        sleep 1
    done
    # Extra sleep for full initialization
    sleep 2
    echo "  [restart] fuse-1 mounted and ready"
}

# ================================================================
# T8.1: Generate manifest, remount, verify manifest
# ================================================================
echo ""
echo "  [T8.1] Write files + manifest + remount + verify"

# Create diverse test files
docker exec "$CONTAINER" mkdir -p "$TDIR/persist"
docker exec "$CONTAINER" sh -c "
dd if=/dev/urandom bs=100 count=1 2>/dev/null > '$TDIR/persist/small.bin'
dd if=/dev/urandom bs=64K count=1 2>/dev/null > '$TDIR/persist/medium.bin'
dd if=/dev/urandom bs=1M count=1 2>/dev/null > '$TDIR/persist/large.bin'
echo 'text content' > '$TDIR/persist/text.txt'
mkdir -p '$TDIR/persist/subdir'
echo 'sub file' > '$TDIR/persist/subdir/file.txt'
"

# Generate manifest BEFORE remount
manifest_before=$(docker exec "$CONTAINER" sh -c \
    "find '$TDIR/persist' -type f -exec md5sum {} \; | sort" 2>/dev/null)

# Remount
restart_fuse

# Generate manifest AFTER remount
manifest_after=$(docker exec "$CONTAINER" sh -c \
    "find '$TDIR/persist' -type f -exec md5sum {} \; | sort" 2>/dev/null)

if [[ "$manifest_before" == "$manifest_after" ]]; then
    pass "T8.1: manifest identical after remount"
else
    fail "T8.1: manifest identical after remount" "match" "mismatch"
    echo "    before (first 5):"; echo "$manifest_before" | head -5 | sed 's/^/      /'
    echo "    after (first 5):";  echo "$manifest_after"  | head -5 | sed 's/^/      /'
fi

# ================================================================
# T8.2: File sizes preserved
# ================================================================
echo ""
echo "  [T8.2] File sizes preserved after remount"

assert_stat "T8.2: small.bin size" "$TDIR/persist/small.bin" '%s' "100"
assert_stat "T8.2: medium.bin size" "$TDIR/persist/medium.bin" '%s' "65536"
assert_stat "T8.2: large.bin size" "$TDIR/persist/large.bin" '%s' "1048576"
assert_stat "T8.2: text.txt size" "$TDIR/persist/text.txt" '%s' "13"

# ================================================================
# T8.3: Metadata (mode/uid/gid) preserved
# ================================================================
echo ""
echo "  [T8.3] Metadata preserved after remount"

f="$TDIR/persist/metadata_test.txt"
docker exec "$CONTAINER" sh -c "echo metadata > '$f'"
docker exec "$CONTAINER" chmod 640 "$f"
docker exec "$CONTAINER" chown 1000:1000 "$f"

restart_fuse

assert_stat "T8.3: mode preserved" "$f" '%a' "640"
assert_stat "T8.3: uid:gid preserved" "$f" '%u %g' "1000 1000"

# ================================================================
# T8.4: Hardlink nlink preserved
# ================================================================
echo ""
echo "  [T8.4] Hardlink nlink preserved after remount"

src="$TDIR/persist/hardlink_src.txt"
hlink="$TDIR/persist/hardlink_dst.txt"
docker exec "$CONTAINER" sh -c "echo hardlink_data > '$src'"
docker exec "$CONTAINER" ln "$src" "$hlink"

# Verify nlink=2 before remount
assert_stat "T8.4: nlink=2 before remount (src)" "$src" '%h' "2"

restart_fuse

assert_stat "T8.4: nlink=2 after remount (src)" "$src" '%h' "2"
assert_stat "T8.4: nlink=2 after remount (dst)" "$hlink" '%h' "2"

# Both files should have same MD5
src_md5=$(docker exec "$CONTAINER" md5sum "$src" 2>/dev/null | awk '{print $1}')
assert_md5_match "T8.4: hardlink content match" "$hlink" "$src_md5"

# Delete source, link should survive
docker exec "$CONTAINER" rm "$src"
assert_not_exists "T8.4: src deleted" "$src"
assert_exists "T8.4: hardlink survives src deletion" "$hlink"
assert_stat "T8.4: nlink=1 after src delete" "$hlink" '%h' "1"

# ================================================================
# T8.5: Symlink target preserved
# ================================================================
echo ""
echo "  [T8.5] Symlink target preserved after remount"

target="$TDIR/persist/sym_target.txt"
link="$TDIR/persist/sym_link.txt"
docker exec "$CONTAINER" sh -c "echo target_data > '$target'"
docker exec "$CONTAINER" ln -s "$target" "$link"

restart_fuse

assert_stat "T8.5: symlink type preserved" "$link" '%F' "symbolic link"
link_target=$(docker exec "$CONTAINER" readlink "$link" 2>/dev/null | tr -d '\r\n')
assert_eq "T8.5: symlink target preserved" "$target" "$link_target"

# Reading through symlink should work
content=$(docker exec "$CONTAINER" cat "$link" 2>/dev/null | tr -d '\r\n')
assert_eq "T8.5: read through symlink" "target_data" "$content"

# ================================================================
# T8.6: Truncate persistence
# ================================================================
echo ""
echo "  [T8.6] Truncate persistence after remount"

f="$TDIR/persist/truncate_test.bin"
docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=200 count=1 of='$f' 2>/dev/null"
docker exec "$CONTAINER" truncate -s 50 "$f"
assert_stat "T8.6: truncated to 50 before remount" "$f" '%s' "50"

restart_fuse

assert_stat "T8.6: still 50 after remount" "$f" '%s' "50"

# ================================================================
# T8.7: Rename persistence
# ================================================================
echo ""
echo "  [T8.7] Rename persistence after remount"

old="$TDIR/persist/rename_old.txt"
new="$TDIR/persist/rename_new.txt"
docker exec "$CONTAINER" sh -c "echo renamedata > '$old'"
docker exec "$CONTAINER" mv "$old" "$new"

restart_fuse

assert_not_exists "T8.7: old path gone after remount" "$old"
assert_exists "T8.7: new path exists after remount" "$new"
content=$(docker exec "$CONTAINER" cat "$new" 2>/dev/null | tr -d '\r\n')
assert_eq "T8.7: renamed file content" "renamedata" "$content"

# ================================================================
# T8.8: Deletion persistence
# ================================================================
echo ""
echo "  [T8.8] Deletion persistence after remount"

f="$TDIR/persist/delete_me.txt"
docker exec "$CONTAINER" sh -c "echo will_be_deleted > '$f'"
assert_exists "T8.8: file exists before delete" "$f"

docker exec "$CONTAINER" rm "$f"

restart_fuse

assert_not_exists "T8.8: file stays deleted after remount" "$f"

# ================================================================
# T8.9: Directory tree persistence
# ================================================================
echo ""
echo "  [T8.9] Directory tree persistence after remount"

d="$TDIR/persist/tree"
docker exec "$CONTAINER" mkdir -p "$d/a/b/c" "$d/x/y"
docker exec "$CONTAINER" sh -c "
echo f1 > '$d/a/f1'
echo f2 > '$d/a/b/f2'
echo f3 > '$d/a/b/c/f3'
echo f4 > '$d/x/y/f4'
"

restart_fuse

# Verify all directories exist
for subdir in "$d" "$d/a" "$d/a/b" "$d/a/b/c" "$d/x" "$d/x/y"; do
    assert_exists "T8.9: dir $subdir exists" "$subdir"
done

# Verify all files and content
assert_eq "T8.9: a/f1 content" "f1" "$(docker exec "$CONTAINER" cat "$d/a/f1" | tr -d '\r\n')"
assert_eq "T8.9: a/b/f2 content" "f2" "$(docker exec "$CONTAINER" cat "$d/a/b/f2" | tr -d '\r\n')"
assert_eq "T8.9: a/b/c/f3 content" "f3" "$(docker exec "$CONTAINER" cat "$d/a/b/c/f3" | tr -d '\r\n')"
assert_eq "T8.9: x/y/f4 content" "f4" "$(docker exec "$CONTAINER" cat "$d/x/y/f4" | tr -d '\r\n')"

# ================================================================
# T8.10: Final full manifest comparison
# ================================================================
echo ""
echo "  [T8.10] Final manifest comparison"

# Generate final manifest of all T8 test files
final_manifest=$(docker exec "$CONTAINER" sh -c \
    "find '$TDIR/persist' -type f -exec md5sum {} \; | sort" 2>/dev/null)

# Count files
file_count=$(echo "$final_manifest" | wc -l)
# Should have: small.bin, medium.bin, large.bin, text.txt, subdir/file.txt,
#   metadata_test.txt, hardlink_dst.txt, sym_target.txt, sym_link.txt,
#   truncate_test.bin, rename_new.txt, tree/a/f1, tree/a/b/f2, tree/a/b/c/f3, tree/x/y/f4
# (hardlink_src.txt and delete_me.txt were deleted)
echo "  Final file count: $file_count"

if [[ "$file_count" -ge 15 ]]; then
    pass "T8.10: sufficient files persisted ($file_count >= 15)"
else
    fail "T8.10: sufficient files persisted" ">= 15" "$file_count"
fi

# ================================================================
# Cleanup T8 data (persist tests done)
# ================================================================
docker exec "$CONTAINER" rm -rf "$TDIR" 2>/dev/null

echo ""
section "Log Check"
check_logs_clean "T8" "fuse-1"
check_logs_clean "T8" "filer-1"

print_summary
