# PowerFS 功能测试严格检查规范

> **适用范围**：T1（VFS 基础）→ T2（正确性）→ T3（布局）→ T4（跨客户端集成）→ T8（持久化）
> **核心原则**：每条功能不仅要"能跑通"，还要**证明结果正确**。任何"看起来对了"但无断言的步骤视为未测试。
> **创建时间**：2026-08-21

---

## 1. 为什么需要严格检查

功能测试阶段如果只验证"命令返回 0"或"文件存在"，会漏掉以下缺陷：

| 看似通过 | 实际缺陷 | 严格检查方法 |
|---------|---------|------------|
| `touch f && ls f` 成功 | size 不为 0 / mode 错误 | `stat -c '%s %a %u %g'` 逐字段断言 |
| `echo data > f && cat f` 返回 data | 写入路径绕过 Filer（本地缓存假象） | 跨客户端 `md5sum` 对比 |
| `chmod 600 f` 返回 0 | mode 未持久化 | remount 后 `stat -c '%a'` 仍为 600 |
| `mkdir d && ls d` 列出 d | nlink 计数错误 / parent_inode 错误 | `stat -c '%h %i'` 断言 nlink + inode |
| `mv a b` 返回 0 | a 仍存在 / b 内容为空 | `test ! -e a && test -e b && md5sum b` |

---

## 2. 通用断言框架

### 2.1 断言函数库（`tests/lib/assertions.sh`）

所有功能测试脚本 source 此库，统一 PASS/FAIL 判定标准：

```bash
#!/usr/bin/env bash
# 严格断言库 — 每个断言失败立即终止当前测试用例

PASS=0; FAIL=0; SKIP=0
C_RED='\033[0;31m'; C_GREEN='\033[0;32m'; C_YELLOW='\033[1;33m'; C_RESET='\033[0m'

pass() { PASS=$((PASS+1)); echo -e "  ${C_GREEN}[PASS]${C_RESET} $1"; }
fail() { FAIL=$((FAIL+1)); echo -e "  ${C_RED}[FAIL]${C_RESET} $1"; echo -e "    ${C_RED}expected:${C_RESET} $2"; echo -e "    ${C_RED}actual:${C_RESET}   $3"; }
skip() { SKIP=$((SKIP+1)); echo -e "  ${C_YELLOW}[SKIP]${C_RESET} $1"; }
section() { echo ""; echo -e "\033[0;36m━━━ $1 ━━━\033[0m"; }

# ---- 核心断言 ----

# 断言两个值完全相等
assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        pass "$desc (='$actual')"
    else
        fail "$desc" "$expected" "$actual"
        return 1
    fi
}

# 断言命令执行成功 (exit code == 0)
assert_ok() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then
        pass "$desc"
    else
        fail "$desc (command failed)" "exit 0" "exit $?"
        return 1
    fi
}

# 断言命令执行失败 (exit code != 0)
assert_fail() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then
        fail "$desc (should have failed)" "exit != 0" "exit 0"
        return 1
    else
        pass "$desc"
    fi
}

# 断言文件/目录存在
assert_exists() {
    local desc="$1" path="$2"
    if docker exec "$CONTAINER" test -e "$path" 2>/dev/null; then
        pass "$desc (exists: $path)"
    else
        fail "$desc" "$path exists" "$path missing"
        return 1
    fi
}

# 断言文件/目录不存在
assert_not_exists() {
    local desc="$1" path="$2"
    if docker exec "$CONTAINER" test ! -e "$path" 2>/dev/null; then
        pass "$desc (absent: $path)"
    else
        fail "$desc" "$path absent" "$path still exists"
        return 1
    fi
}

# 断言 MD5 一致（跨容器对比的核心）
assert_md5_match() {
    local desc="$1" path="$2" expected_md5="$3"
    local actual_md5
    actual_md5=$(docker exec "$CONTAINER" md5sum "$path" 2>/dev/null | awk '{print $1}')
    if [[ "$actual_md5" == "$expected_md5" ]]; then
        pass "$desc (md5=${actual_md5:0:12}...)"
    else
        fail "$desc" "$expected_md5" "$actual_md5"
        return 1
    fi
}

# 断言 stat 字段（size/mode/uid/gid/nlink/mtime）
# 用法: assert_stat "desc" /path '%s %a %u %g %h' "100 644 0 0 1"
assert_stat() {
    local desc="$1" path="$2" fmt="$3" expected="$4"
    local actual
    actual=$(docker exec "$CONTAINER" stat -c "$fmt" "$path" 2>/dev/null | tr -d '\r')
    if [[ "$actual" == "$expected" ]]; then
        pass "$desc (stat='$actual')"
    else
        fail "$desc" "$expected" "$actual"
        return 1
    fi
}

# 断言跨容器 MD5 一致
assert_md5_cross() {
    local desc="$1" path="$2" container_a="$3" container_b="$4"
    local md5_a md5_b
    md5_a=$(docker exec "$container_a" md5sum "$path" 2>/dev/null | awk '{print $1}')
    md5_b=$(docker exec "$container_b" md5sum "$path" 2>/dev/null | awk '{print $1}')
    if [[ -n "$md5_a" && "$md5_a" == "$md5_b" ]]; then
        pass "$desc (both=${md5_a:0:12}...)"
    else
        fail "$desc" "md5 match ($container_a=$md5_a, $container_b=$md5_b)" "mismatch"
        return 1
    fi
}

# 汇总打印
print_summary() {
    echo ""
    echo "━━━ Summary ━━━"
    echo -e "  ${C_GREEN}PASS${C_RESET}: $PASS"
    echo -e "  ${C_RED}FAIL${C_RESET}: $FAIL"
    echo -e "  ${C_YELLOW}SKIP${C_RESET}: $SKIP"
    if [[ $FAIL -gt 0 ]]; then
        echo -e "  ${C_RED}RESULT: FAILED${C_RESET}"
        return 1
    else
        echo -e "  ${C_GREEN}RESULT: ALL PASS${C_RESET}"
        return 0
    fi
}
```

### 2.2 测试隔离原则

```bash
# 每个测试套件必须使用唯一时间戳目录，避免相互干扰
TSTAMP=$(date +%s)
TDIR="/mnt/powerfs/func_${TSTAMP}"

# 测试前：创建隔离目录
setup() {
    docker exec "$CONTAINER" mkdir -p "$TDIR"
}

# 测试后：清理（即使中途失败也要清理）
teardown() {
    docker exec "$CONTAINER" rm -rf "$TDIR" 2>/dev/null || true
}
trap teardown EXIT
```

---

## 3. 每条功能的严格检查标准

### 3.1 T1 VFS 基础操作

#### T1.1 文件 CRUD

| 操作 | 严格检查项 | 断言方式 |
|------|-----------|---------|
| create（touch） | 文件存在 + size=0 + mode=644 + type=regular | `assert_stat "create" $f '%s %a %F' "0 644 regular file"` |
| write（echo/dd） | 写入后 size 正确 + 内容 MD5 与源一致 | `assert_stat "size" $f '%s' "$N"` + `assert_md5_match "content" $f "$src_md5"` |
| read（cat） | 读回 MD5 与写入时一致 | 对比 `dd + md5sum` |
| truncate（缩小） | size 变小 + 截断部分数据丢失 | `assert_stat "trunc" $f '%s' "$new_size"` + 读回 MD5 匹配截断后内容 |
| truncate（扩展） | size 变大 + 扩展部分为 \0 | `assert_stat "extend" $f '%s' "$big_size"` + `hexdump` 验证尾部全 0 |
| overwrite | 覆盖后 size 正确 + 旧数据完全消失 | 写入新内容 → MD5 不等于旧内容 → 等于新内容 |
| append | size 累加 + 内容 = 旧+新 | `cat old new > expected && assert_md5_match` |

**关键：不能只看 exit code，必须验证 stat 字段 + MD5 内容一致性。**

#### T1.2 目录操作

| 操作 | 严格检查项 | 断言方式 |
|------|-----------|---------|
| mkdir | 目录存在 + type=directory + mode=755 | `assert_stat "mkdir" $d '%a %F' "755 directory"` |
| mkdir -p | 嵌套目录存在 + 中间目录也存在 | 逐层 `test -d` |
| rmdir | 目录不存在 + ENOENT | `assert_not_exists "rmdir" $d` |
| rmdir（非空） | 失败 + ENOTEMPTY | `assert_fail "rmdir non-empty" rmdir $nonempty` |
| readdir（ls） | 条目数量 + 名称完全匹配 | `ls -1 $d \| sort` 对比预期列表 |
| rename（文件） | 旧路径不存在 + 新路径存在 + 内容 MD5 一致 | `assert_not_exists` + `assert_exists` + `assert_md5_match` |
| rename（目录） | 旧路径不存在 + 新路径存在 + 子文件树完整 | `diff -r` 对比 |
| unlink | 文件不存在 + ENOENT | `assert_not_exists "unlink" $f` |
| symlink | 链接存在 + type=symlink + readlink 值正确 | `assert_stat '%F' "symbolic link"` + `readlink` 对比 |
| hardlink | nlink=2 + 两路径 MD5 一致 + 删除源后链接存活 | `assert_stat '%h' "2"` + `assert_md5_match` + unlink 源 + `assert_exists` |

**nlink 计数是高频 bug 来源，必须每次硬链接操作后断言 `%h` 字段。**

#### T1.3 权限

| 操作 | 严格检查项 |
|------|-----------|
| chmod 600 | `stat -c '%a'` == "600" + remount 后仍为 600 |
| chmod 755 | `stat -c '%a'` == "755" |
| chown uid:gid | `stat -c '%u %g'` == "1000 1000" |
| utimes | `stat -c '%Y'`（mtime）== 指定时间戳 |

**权限测试必须包含跨客户端验证：fuse-1 chmod → fuse-2 stat 看到新 mode。**

#### T1.4 特殊文件

| 操作 | 严格检查项 |
|------|-----------|
| mkfifo | `test -p` 为 true + `stat -c '%F'` == "fifo" |
| socket | `test -S` 为 true |
| mknod（char） | `test -c` 为 true（需 root） |

#### T1.5 边界

| 场景 | 严格检查项 |
|------|-----------|
| 空文件 | size=0 + 可读（返回空）+ 可写 |
| 255B 文件名 | 创建成功 + `stat` 存在 + `ls` 列出 |
| 256B 文件名 | 创建失败 + ENAMETOOLONG |
| 空格文件名 | 创建成功 + `stat` 存在 + 引号访问正确 |
| 中文文件名 | 创建成功 + `stat` 存在 + `ls` 列出 |

#### T1.6 并发

| 场景 | 严格检查项 |
|------|-----------|
| 4 进程写不同文件 | 每个文件 MD5 与各自源一致 + 无 corruption |
| 4 进程读同一文件 | 4 个读结果 MD5 完全一致 |

### 3.2 T2 文件系统正确性

| 测试 | 严格检查项 |
|------|-----------|
| cp -r（1000+ 文件） | `diff -r src dst` 无差异 + 文件计数一致 |
| tar czf + tar xzf | 解压后 `find + md5sum` 清单完全一致 |
| 源码编译 | 编译 exit 0 + 无 IO error 日志 + 产物存在 |
| rsync -a | `rsync --checksum --dry-run` 无增量项 |
| git clone + commit | `git status` clean + `git fsck` 无错误 |

**T2 的核心是 `diff -r` 和 `find + md5sum` 清单对比，不能只看命令 exit code。**

### 3.3 T3 布局功能

| 测试 | 严格检查项 |
|------|-----------|
| K1 Flat | 写入 → 读回 MD5 一致 + 跨客户端 MD5 一致 + Filer 日志显示 chunks >= 1 |
| K2 Inline | 小文件 Filer 日志 `inline_len > 0` + 读回 MD5 一致 + 超阈值后迁移到 chunks |
| K3 Stripe | 跨多卷写入 → 读回 MD5 一致 + 跨客户端 MD5 一致 |
| K4 Replicated | 写入 → 读回 MD5 一致 + 跨客户端 MD5 一致（仅正常路径，无故障注入） |

**T3 每条必须包含跨客户端 MD5 对比（fuse-1 写 → fuse-2 读），证明数据确实经过 Filer 而非本地缓存。**

### 3.4 T4 跨客户端集成

| 测试 | 严格检查项 |
|------|-----------|
| FUSE→FUSE 同构 | fuse-1 写 → fuse-2 `md5sum` 一致 |
| FUSE→Kernel 异构 | fuse-1 写 → VM 内 `md5sum` 一致 |
| Kernel→FUSE 异构 | VM 写 → fuse-1 `md5sum` 一致 |
| remount 一致性 | 重启容器后 `find + md5sum` 清单不变 |
| 并发读写 | 两客户端各写不同文件 → 交叉读 MD5 一致 |

**T4 的核心是 `assert_md5_cross`：每次都必须在两个容器分别计算 MD5 并对比。**
**跨客户端读取前必须 drop cache（`echo 2 > /proc/sys/vm/drop_caches`），避免读本地页缓存。**

### 3.5 T8 持久化

| 测试 | 严格检查项 |
|------|-----------|
| 写入持久化 | 写入 → remount → `md5sum` 一致 |
| 删除持久化 | 删除 → remount → `test ! -e` |
| 硬链接持久化 | remount 后 nlink 仍为 2 + 删除源后链接存活 |
| 软链接持久化 | remount 后 `readlink` 值正确 |
| truncate 持久化 | remount 后 size + 内容一致 |
| 元数据持久化 | remount 后 mode/uid/gid/mtime 一致 |
| rename 持久化 | remount 后旧路径 ENOENT + 新路径可读 |
| 综合场景 | remount 后 `find + md5sum` manifest 完全一致 |

**T8 每条都涉及 remount（`docker restart` 或 `umount + mount`），remount 后的断言必须用 manifest 对比，不能只抽查单个文件。**

---

## 4. 防紊乱机制

### 4.1 测试前置检查（每次运行前必须通过）

```bash
preflight() {
    section "Preflight: Environment Check"

    # 1. 所有容器运行中
    for c in master-1 master-2 master-3 filer-1 filer-2 filer-3 \
             volume-1 volume-2 volume-3 fuse-1 fuse-2; do
        assert_ok "container $c running" docker inspect -f '{{.State.Running}}' $c
    done

    # 2. 挂载点可访问
    docker exec fuse-1 test -d /mnt/powerfs || { echo "FATAL: fuse-1 mount missing"; exit 1; }
    docker exec fuse-2 test -d /mnt/powerfs || { echo "FATAL: fuse-2 mount missing"; exit 1; }

    # 3. 基础读写验证（一个 round-trip）
    docker exec fuse-1 sh -c 'echo preflight > /mnt/powerfs/.preflight && cat /mnt/powerfs/.preflight' \
        | grep -q preflight || { echo "FATAL: basic write+read failed"; exit 1; }

    # 4. 跨客户端可见性
    docker exec fuse-2 cat /mnt/powerfs/.preflight 2>/dev/null | grep -q preflight \
        || { echo "FATAL: cross-client visibility failed"; exit 1; }

    # 5. 清理 preflight 文件
    docker exec fuse-1 rm -f /mnt/powerfs/.preflight
}
```

### 4.2 测试用例隔离规则

1. **唯一目录**：每个测试套件使用 `/mnt/powerfs/func_<tstamp>_<stage>/` 隔离目录
2. **trap 清理**：`trap 'rm -rf $TDIR' EXIT` 确保中途失败也清理
3. **不依赖前序状态**：每个测试自己创建所需的文件/目录，不假设上个测试的残留
4. **失败即停**：单个断言失败后 `return 1`，不继续执行依赖该断言的后续步骤

### 4.3 日志检查（防止"静默错误"）

每个测试套件结束后检查 FUSE/Filer 日志中的错误：

```bash
check_logs_clean() {
    local stage="$1"
    local errors

    # FUSE 日志
    errors=$(docker logs fuse-1 2>&1 | tail -200 | \
        grep -iE 'error|panic|deadlock|unwrap|failed' | \
        grep -v 'grep' || true)
    if [[ -n "$errors" ]]; then
        fail "$stage: fuse-1 log has errors"
        echo "$errors" | head -10 | sed 's/^/    /'
    fi

    # Filer 日志
    errors=$(docker logs filer-1 2>&1 | tail -200 | \
        grep -iE 'error|panic|deadlock|unwrap|failed' | \
        grep -v 'grep' || true)
    if [[ -n "$errors" ]]; then
        fail "$stage: filer-1 log has errors"
        echo "$errors" | head -10 | sed 's/^/    /'
    fi
}
```

### 4.4 Drop Cache 规则

**跨客户端读取前必须 drop cache**，否则读到的是本地页缓存而非 Filer 数据：

```bash
# 在读取端容器内执行
drop_cache() {
    docker exec "$1" sh -c 'sync; echo 2 > /proc/sys/vm/drop_caches' 2>/dev/null || true
}

# 用法：fuse-1 写入 → drop cache → fuse-2 读取
fuse1 "echo data > /mnt/powerfs/testfile"
drop_cache fuse-2
md5_on_fuse2=$(fuse2 "md5sum /mnt/powerfs/testfile")
```

---

## 5. 测试脚本模板

```bash
#!/usr/bin/env bash
# tests/functional/t1_vfs_basic.sh
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

# ---- T1.1a: create empty file ----
echo "  [T1.1a] create empty file"
f="$TDIR/empty.txt"
assert_ok "touch creates file" docker exec "$CONTAINER" touch "$f"
assert_exists "file exists" "$f"
assert_stat "empty file stat" "$f" '%s %a %F' "0 644 regular file"

# ---- T1.1b: write 100B + MD5 verify ----
echo "  [T1.1b] write 100B + MD5"
f="$TDIR/write100.bin"
src_md5=$(docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=100 count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "100B size" "$f" '%s' "100"
assert_md5_match "100B content" "$f" "$src_md5"

# ---- T1.1c: cross-client MD5 (防本地缓存假象) ----
echo "  [T1.1c] cross-client MD5"
drop_cache fuse-2
assert_md5_cross "fuse-1→fuse-2 same MD5" "$f" "fuse-1" "fuse-2"

# ---- Summary ----
check_logs_clean "T1"
print_summary
```

---

## 6. 执行顺序与门禁

```
T1.1 → T1.2 → T1.3 → T1.4 → T1.5 → T1.6
  ↓（全部 PASS 才进下一阶段）
T2.1 → T2.2 → ... → T2.5
  ↓
T3.1 → T3.2 → T3.3 → T3.4
  ↓
T4.1 → T4.2 → T4.3 → T4.4
  ↓
T8.1 → T8.2 → ... → T8.10
```

**门禁规则**：
- 同阶段内任一测试 FAIL → 修复后重跑该阶段全部测试（不只重跑失败的）
- 阶段间不允许跳级（T1 未全 PASS 不许跑 T2）
- 每个测试脚本退出码 = FAIL 数 > 0 ? 1 : 0，CI 中非零退出码阻断流水线
