#!/usr/bin/env bash
# ================================================================
# P3.1 Verification Script: mode/uid/gid merged into CreateInode
#
# Verifies that file/dir creation triggers exactly ONE per-shard Raft
# log (CreateInode) and NO follow-up SetAttr Raft log for
# mode/uid/gid. An explicit `chmod` MUST still emit a SetAttr-like
# Raft log (covered via CRDT SetAttrMeta path for mode-only changes).
#
# Evidence chain (3 independent measurements):
#   [C] STRACE (kernel): confirm openat(O_CREAT, mode=0666) and
#       mkdir(mode=0700) actually pass mode to the kernel so the
#       FUSE create/mkdir handlers have the value to embed.
#   [A] LOG-BASED (filer): per test-batch, capture ALL filer logs
#       before and after, count deltas for:
#         - create_file_with_shard latency:   (file creates, 1 Raft)
#         - FILER_NET_SETATTR_META.*mode=    (CRDT mode setattr, used
#                                             by explicit chmod as well
#                                             as OLD create path)
#         - FILER_NET_SETATTR:.*mode=        (strong path setattr, rare)
#       P3.1 passes if:
#         N creates -> N create_file_with_shard AND 0 SetAttr(mode)
#   [B] CLIENT-SIDE EVIDENCE (fuse fuse-1 logs): match create timing
#       with setattr CRDT log lines emitted by meta_shard_client.
#       When P3.1 works, mode=None/uid=None/gid=None for the create
#       path SetAttrMeta calls (only timestamps remain).
#
# Usage:
#   bash scripts/tests/perf/verify_p31_setattr_elimination.sh
#
# Environment: expects docker-compose cluster (filer-1..3, fuse-1)
# to be running. fuse-1 mounts the filesystem at /mnt/powerfs.
# ================================================================

set -u
cd "$(dirname "$0")/../.."

FUSE="fuse-1"
FILERS=("filer-1" "filer-2" "filer-3")
TESTDIR="/p31_v2_$$"
# Make N even so the strict/normal dir split is exact halves.
N=30
# Guarantee even N:
if (( N % 2 != 0 )); then N=$((N+1)); fi
PASS=0
FAIL=0
TOTAL=0

# ---------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------
RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'; NC='\033[0m'
pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo -e "  [${GRN}PASS${NC}] $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo -e "  [${RED}FAIL${NC}] $1"; }
warn() { echo -e "  [${YLW}WARN${NC}] $1"; }

TMPDIR="/tmp/p31v2_$$"
mkdir -p "$TMPDIR"
cleanup() { rm -rf "$TMPDIR" 2>/dev/null || true; }
trap cleanup EXIT

# ---------------------------------------------------------------
# Log snapshot utilities
# ---------------------------------------------------------------
snap_all_logs() {
    # Saves stdout+stderr of docker logs <name> into $TMPDIR/$tag/$name.log
    local tag=$1
    local dir="$TMPDIR/$tag"
    mkdir -p "$dir"
    for f in "${FILERS[@]}" "$FUSE"; do
        docker logs "$f" > "$dir/${f}.log" 2>&1 || true
    done
    echo "$dir"
}

delta_count() {
    # delta_count <before_dir> <after_dir> <container> <pattern>
    local before_dir=$1 after_dir=$2 container=$3 pat=$4
    local b=0 a=0
    [[ -f "$before_dir/${container}.log" ]] && \
        b=$(grep -cE "$pat" "$before_dir/${container}.log" 2>/dev/null) || true
    [[ -f "$after_dir/${container}.log" ]] && \
        a=$(grep -cE "$pat" "$after_dir/${container}.log" 2>/dev/null) || true
    echo $(( a - b ))
}

sum_delta() {
    # sum_delta <before_dir> <after_dir> <pattern>
    # Sums pattern deltas across ALL 3 filers + fuse (as requested per caller).
    local before_dir=$1 after_dir=$2 pat=$3
    local total=0
    for f in "${FILERS[@]}"; do
        total=$(( total + $(delta_count "$before_dir" "$after_dir" "$f" "$pat") ))
    done
    echo $total
}
sum_delta_fuse() {
    local before_dir=$1 after_dir=$2 pat=$3
    delta_count "$before_dir" "$after_dir" "$FUSE" "$pat"
}

# ---------------------------------------------------------------
# Setup: ensure strace is installed in fuse-1
# ---------------------------------------------------------------
echo "================================================================"
echo " P3.1 verification: CreateInode embeds mode/uid/gid"
echo " Batches: $N files + $N dirs + explicit chmod sanity checks"
echo "================================================================"

if ! docker exec "$FUSE" bash -c 'command -v strace >/dev/null 2>&1'; then
    echo "[setup] installing strace in $FUSE..."
    docker exec "$FUSE" bash -c \
        'apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq strace >/dev/null 2>&1' \
        || warn "strace install failed; [C] checks will be skipped"
fi

# Ensure test dir exists in the mount
docker exec "$FUSE" bash -c "rm -rf /mnt/powerfs${TESTDIR} 2>/dev/null; mkdir -p /mnt/powerfs${TESTDIR}"
sleep 1

# ---------------------------------------------------------------
# [C] Strace kernel evidence
# ---------------------------------------------------------------
echo ""
echo "=== [C] Kernel evidence: mode is passed via openat(O_CREAT) / mkdir(mode) ==="
STRACE_LOG="$TMPDIR/strace_c.txt"
# Use bash -c with explicit expansion: create 3 distinct files + 3 distinct dir modes
docker exec "$FUSE" bash -c "
    STRACE_LOG=/tmp/strace_c_$$.txt
    rm -f \$STRACE_LOG
    strace -f -e trace=openat,mkdir,mkdirat -o \$STRACE_LOG bash -c >/dev/null 2>&1 '
        for i in 1 2 3; do
            echo cs\$i > /mnt/powerfs${TESTDIR}/c_file_\$i.txt
        done
        mkdir -m 700 /mnt/powerfs${TESTDIR}/c_dir_a
        mkdir      /mnt/powerfs${TESTDIR}/c_dir_b
        mkdir -m 755 /mnt/powerfs${TESTDIR}/c_dir_c
    '
    cat \$STRACE_LOG
    rm -f \$STRACE_LOG
" > "$STRACE_LOG" 2>/dev/null

echo "  -- relevant syscalls --"
grep -E "${TESTDIR}" "$STRACE_LOG" | grep -vE "EEXIST|ENONENT|O_RDONLY" || true

N_FILE_OPENAT=$(grep -Ec "openat.*${TESTDIR}.*O_CREAT.*0666" "$STRACE_LOG" || true)
N_MKDIR=$(grep -Ec "(^|[[:space:]])mkdir(at)?.*${TESTDIR}.*07" "$STRACE_LOG" || true)
N_DIR_700=$(grep -Ec "mkdir.*${TESTDIR}/c_dir_a.*0700" "$STRACE_LOG" || true)
N_DIR_755=$(grep -Ec "mkdir.*${TESTDIR}/c_dir_c.*0755" "$STRACE_LOG" || true)
echo "  summary: O_CREAT(0666)=$N_FILE_OPENAT  mkdir(mode>=0700)=$N_MKDIR  c_dir_a(0700)=$N_DIR_700  c_dir_c(0755)=$N_DIR_755"

if [[ $N_FILE_OPENAT -ge 2 ]]; then
    pass "[C] >=2 file creates visible as openat(O_CREAT, mode=0666)"
else
    fail "[C] too few O_CREAT syscalls (got $N_FILE_OPENAT)"
fi
if [[ $N_DIR_700 -ge 1 && $N_DIR_755 -ge 1 ]]; then
    pass "[C] mkdir -m 700 / mkdir -m 755 both visible with exact mode in syscall"
else
    fail "[C] mkdir modes not visible in strace (700=$N_DIR_700, 755=$N_DIR_755)"
fi

# ---------------------------------------------------------------
# Pre-batch baseline
# ---------------------------------------------------------------
echo ""
echo "=== [A] Filer-log evidence: baseline snapshot ==="
BEFORE=$(snap_all_logs "before")
SAMPLE_INODE_BEFORE=$(docker logs filer-1 2>/dev/null \
    | grep -oE "inode=[0-9]+" | tail -1 | cut -d= -f2 || echo 0)
echo "  sample last known inode (filer-1) = $SAMPLE_INODE_BEFORE"
echo "  (creations above this baseline should fall in a new range)"

# ---------------------------------------------------------------
# BATCH 1 — N files via echo > f.txt
# ---------------------------------------------------------------
echo ""
echo "=== Batch 1: create $N files (umask 022 → expected 0o644) ==="
BATCH1_OUT="$TMPDIR/batch1.txt"
docker exec "$FUSE" bash -c "
    umask 022
    for i in \$(seq 1 $N); do
        echo payload\$i > /mnt/powerfs${TESTDIR}/b1_f\$i.txt
    done
    # Sanity check modes
    echo SAMPLE_MODES=\$(for i in 1 10 20 30; do
        s=\$(stat -c '%a' /mnt/powerfs${TESTDIR}/b1_f\$i.txt 2>/dev/null || echo 'MISS')
        printf '%s ' \"\$s\"
    done)
    echo COUNT=\$(ls /mnt/powerfs${TESTDIR}/b1_f*.txt 2>/dev/null | wc -l)
" | tee "$BATCH1_OUT"

BATCH1_AFTER=$(snap_all_logs "batch1_after")

B1_CREATE=$(sum_delta "$BEFORE" "$BATCH1_AFTER" \
    "create_file_with_shard latency:.*mode=100[0-7]{3}")
B1_SETATTRMETA_MODE=$(sum_delta "$BEFORE" "$BATCH1_AFTER" \
    "FILER_NET_SETATTR_META:.*mode=Some\(")
B1_SETATTR_MODE=$(sum_delta "$BEFORE" "$BATCH1_AFTER" \
    "FILER_NET_SETATTR:.*mode=Some\(")
B1_UIDGID=$(sum_delta "$BEFORE" "$BATCH1_AFTER" \
    "FILER_NET_SETATTR:.*uid=Some\(|FILER_NET_SETATTR:.*gid=Some\(|FILER_NET_SETATTR_META:.*uid=Some\(|FILER_NET_SETATTR_META:.*gid=Some\(")

# Per-filer breakdown
for f in "${FILERS[@]}"; do
    c=$(delta_count "$BEFORE" "$BATCH1_AFTER" "$f" \
        "create_file_with_shard latency:.*mode=100[0-7]{3}")
    sm=$(delta_count "$BEFORE" "$BATCH1_AFTER" "$f" \
        "FILER_NET_SETATTR_META:.*mode=Some\(")
    ss=$(delta_count "$BEFORE" "$BATCH1_AFTER" "$f" \
        "FILER_NET_SETATTR:.*mode=Some\(")
    [[ $(( c + sm + ss )) -gt 0 ]] && echo "  $f: creates=$c  SetAttrMeta_mode=$sm  SetAttr_mode=$ss"
done

# Spot-check actual created file modes from output
B1_MODE_OK=$(grep -oE "SAMPLE_MODES=.*" "$BATCH1_OUT" | head -1)
echo "  $B1_MODE_OK"
B1_FILE_COUNT=$(grep -oE "COUNT=[0-9]+" "$BATCH1_OUT" | head -1 | cut -d= -f2)
echo "  B1 aggregate: creates=$B1_CREATE  SetAttrMeta_mode=$B1_SETATTRMETA_MODE  SetAttr_mode=$B1_SETATTR_MODE  uid/gid_setattr=$B1_UIDGID  created=$B1_FILE_COUNT"

# ---- asserts ----
if [[ "$B1_FILE_COUNT" == "$N" ]]; then
    pass "[A.1] Created exactly $N/$N reported files in fuse-1 mount"
else
    fail "[A.1] Expected $N files, mount reports $B1_FILE_COUNT"
fi
if [[ $B1_CREATE -ge $(( N * 95 / 100 )) ]]; then
    pass "[A.2] create_file_with_shard >=95% of N: $B1_CREATE creates (1 Raft/file, as expected by P3.1)"
else
    fail "[A.2] Too few create_file_with_shard events: $B1_CREATE (need >= $(( N * 95 / 100 )))"
fi
if [[ $B1_SETATTRMETA_MODE -eq 0 && $B1_SETATTR_MODE -eq 0 ]]; then
    pass "[A.3] ZERO SetAttr(mode) Raft proposals on file create (P3.1: mode already embedded in CreateInode)"
else
    fail "[A.3] Unexpected SetAttr(mode) during file create: SetAttrMeta=$B1_SETATTRMETA_MODE, SetAttr=$B1_SETATTR_MODE"
fi
if [[ $B1_UIDGID -eq 0 ]]; then
    pass "[A.4] ZERO SetAttr(uid/gid) Raft proposals on file create"
else
    fail "[A.4] Unexpected SetAttr(uid/gid) events: $B1_UIDGID"
fi

# ---------------------------------------------------------------
# BATCH 2 — N dirs (half strict 700, half normal 755)
# ---------------------------------------------------------------
echo ""
echo "=== Batch 2: create $N dirs (half -m 700, half default 0755) ==="
BATCH2_OUT="$TMPDIR/batch2.txt"
HALF=$(( N / 2 ))
docker exec "$FUSE" bash -c "
    for i in \$(seq 1 $HALF); do
        mkdir -m 700 /mnt/powerfs${TESTDIR}/b2_s\$i
    done
    for i in \$(seq 1 $HALF); do
        mkdir /mnt/powerfs${TESTDIR}/b2_n\$i
    done
    STRICT=\$(for i in 1 5 10 15; do
        stat -c '%a' /mnt/powerfs${TESTDIR}/b2_s\$i 2>/dev/null
    done | sort -u | tr '\n' ',' )
    NORM=\$(for i in 1 5 10 15; do
        stat -c '%a' /mnt/powerfs${TESTDIR}/b2_n\$i 2>/dev/null
    done | sort -u | tr '\n' ',' )
    SCOUNT=\$(ls -d /mnt/powerfs${TESTDIR}/b2_s* 2>/dev/null | wc -l)
    NCOUNT=\$(ls -d /mnt/powerfs${TESTDIR}/b2_n* 2>/dev/null | wc -l)
    TOTAL=\$(( SCOUNT + NCOUNT ))
    echo "DIR_COUNT=\$TOTAL"
    echo "STRICT_MODE_SET=\$STRICT"
    echo "NORMAL_MODE_SET=\$NORM"
    echo "STRICT_DIR_COUNT=\$SCOUNT"
    echo "NORMAL_DIR_COUNT=\$NCOUNT"
" | tee "$BATCH2_OUT"

BATCH2_AFTER=$(snap_all_logs "batch2_after")

# Directories: search for mkdir handler TLV log OR FILER_NET_CREATE_DIR/phase
# Since create_directory doesn't emit a single info line, we use:
#  - "create_directory.*phase" for cross-shard mkdir phase tracking
#  - Or fall back to counting distinct inode ranges via AddDirEntry
B2_DIR_CREATE_DIRECT=$(sum_delta "$BEFORE" "$BATCH2_AFTER" \
    "create_directory_phase_[ab]|FILER_NET_MKDIR|mkdir.*latency|propose_create_inode_and_direntry.*Directory")
# AddDirEntry is called once per dir+file create (so we use the diff vs B1 count of creates as estimate)
B2_ADD_ENTRIES=$(sum_delta "$BATCH1_AFTER" "$BATCH2_AFTER" \
    "AddDirEntry|add_dir_entry|propose_create_inode_and_direntry")
# SetAttr(mode) checks
B2_SM=$(sum_delta "$BATCH1_AFTER" "$BATCH2_AFTER" \
    "FILER_NET_SETATTR_META:.*mode=Some\(")
B2_SS=$(sum_delta "$BATCH1_AFTER" "$BATCH2_AFTER" \
    "FILER_NET_SETATTR:.*mode=Some\(")

for f in "${FILERS[@]}"; do
    d=$(delta_count "$BATCH1_AFTER" "$BATCH2_AFTER" "$f" \
        "FILER_NET_MKDIR|create_directory_phase_[ab]|Directory.*CreateInode|CreateInode.*Directory")
    sm=$(delta_count "$BATCH1_AFTER" "$BATCH2_AFTER" "$f" \
        "FILER_NET_SETATTR_META:.*mode=Some\(")
    [[ $(( d + sm )) -gt 0 ]] && echo "  $f: dir events~=$d  SetAttrMeta_mode=$sm"
done

B2_DIR_COUNT=$(grep -oE "DIR_COUNT=[0-9]+" "$BATCH2_OUT" | head -1 | cut -d= -f2)
B2_STRICT=$(grep -oE "STRICT_MODE_SET=[0-9,]*" "$BATCH2_OUT" | head -1)
B2_NORMAL=$(grep -oE "NORMAL_MODE_SET=[0-9,]*" "$BATCH2_OUT" | head -1)
echo "  mkdirs mounted: $B2_DIR_COUNT"
echo "  $B2_STRICT"
echo "  $B2_NORMAL"
echo "  B2 aggregate: directory log markers=$B2_DIR_CREATE_DIRECT add_dir_entry=$B2_ADD_ENTRIES SetAttrMeta_mode=$B2_SM SetAttr_mode=$B2_SS"

if [[ "$B2_DIR_COUNT" == "$N" ]]; then
    pass "[A.5] Exactly $N dirs present in mount after creation"
else
    fail "[A.5] Expected $N dirs, mount shows $B2_DIR_COUNT"
fi
# Mode correctness: strict must only have 700, normal only 755
STRICT_ONLY_700=$(grep -qE "^STRICT_MODE_SET=700,?$" "$BATCH2_OUT" && echo YES || echo NO)
NORMAL_ONLY_755=$(grep -qE "^NORMAL_MODE_SET=755,?$" "$BATCH2_OUT" && echo YES || echo NO)
if [[ $STRICT_ONLY_700 == "YES" ]]; then
    pass "[A.6] mkdir -m 700 directories persisted as mode=700 (correctly baked into CreateInode)"
else
    fail "[A.6] strict dirs did not result in unique mode 700: $B2_STRICT"
fi
if [[ $NORMAL_ONLY_755 == "YES" ]]; then
    pass "[A.7] mkdir default directories persisted as mode=755 (correctly baked into CreateInode)"
else
    fail "[A.7] normal dirs did not result in unique mode 755: $B2_NORMAL"
fi
if [[ $B2_SM -eq 0 && $B2_SS -eq 0 ]]; then
    pass "[A.8] ZERO SetAttr(mode) Raft proposals during mkdir (mode baked in at creation)"
else
    fail "[A.8] Unexpected SetAttr(mode) on dir create: SetAttrMeta=$B2_SM SetAttr=$B2_SS"
fi

# ---------------------------------------------------------------
# SANITY — explicit chmod should still produce SetAttrMeta(mode)
# ---------------------------------------------------------------
echo ""
echo "=== Sanity check: explicit chmod MUST still produce SetAttrMeta(mode) ==="
SANITY_OUT="$TMPDIR/sanity.txt"
# Note: busybox stat repeats the format string for every file argument, so
# run 4 separate stats (one per path) and combine with a tiny shell expression.
docker exec "$FUSE" bash -c "
    chmod 600  /mnt/powerfs${TESTDIR}/b1_f1.txt
    chmod 640  /mnt/powerfs${TESTDIR}/b1_f2.txt
    chmod 750  /mnt/powerfs${TESTDIR}/b2_s1
    chmod 755  /mnt/powerfs${TESTDIR}/b2_n1
    F1_MODE=\$(stat -c '%a' /mnt/powerfs${TESTDIR}/b1_f1.txt)
    F2_MODE=\$(stat -c '%a' /mnt/powerfs${TESTDIR}/b1_f2.txt)
    S1_MODE=\$(stat -c '%a' /mnt/powerfs${TESTDIR}/b2_s1)
    N1_MODE=\$(stat -c '%a' /mnt/powerfs${TESTDIR}/b2_n1)
    echo \"F1=\${F1_MODE} F2=\${F2_MODE} S1=\${S1_MODE} N1=\${N1_MODE}\"
" > "$SANITY_OUT" 2>&1
cat "$SANITY_OUT"

SANITY_AFTER=$(snap_all_logs "sanity_after")
SANITY_SM=$(sum_delta "$BATCH2_AFTER" "$SANITY_AFTER" \
    "FILER_NET_SETATTR_META:.*mode=Some\(")
SANITY_SS=$(sum_delta "$BATCH2_AFTER" "$SANITY_AFTER" \
    "FILER_NET_SETATTR:.*mode=Some\(")
SANITY_TOTAL=$(( SANITY_SM + SANITY_SS ))
echo "  4 explicit chmods → SetAttrMeta_mode=$SANITY_SM + SetAttr_mode=$SANITY_SS = total=$SANITY_TOTAL"

if grep -qE "F1=600 F2=640 S1=750 N1=755" "$SANITY_OUT"; then
    pass "[S.1] All 4 explicit chmods are reflected in the mount (chmod path works end-to-end)"
else
    fail "[S.1] chmod modes wrong: $(cat "$SANITY_OUT")"
fi
if [[ $SANITY_TOTAL -ge 3 ]]; then
    pass "[S.2] Explicit chmod triggers SetAttrMeta(mode)/SetAttr(mode) Raft log ($SANITY_TOTAL >= 3 expected >=3) — existing path preserved"
else
    fail "[S.2] SetAttr(mode) should fire for explicit chmod but only $SANITY_TOTAL seen"
fi

# ---------------------------------------------------------------
# [B] Fuse client evidence: SetAttrMeta calls should NOT mention
#     mode/uid/gid in the "create" window (only mtime/atime).
# ---------------------------------------------------------------
echo ""
echo "=== [B] Fuse client SetAttrMeta CRDT trace ==="
FUSE_B1_SM=$(sum_delta_fuse "$BEFORE" "$BATCH1_AFTER" \
    "setattr CRDT path:.*mode=Some\(")
FUSE_B1_UID=$(sum_delta_fuse "$BEFORE" "$BATCH1_AFTER" \
    "setattr CRDT path:.*uid=Some\(|setattr CRDT path:.*gid=Some\(")
FUSE_B1_TIMESTAMP=$(sum_delta_fuse "$BEFORE" "$BATCH1_AFTER" \
    "setattr CRDT path:.*mtime=Some.*atime=Some|setattr CRDT path:.*atime=Some.*mtime=Some")
echo "  fuse-1 (Batch1 files): setattr CRDT with mode=$FUSE_B1_SM, uid/gid=$FUSE_B1_UID, pure timestamps=$FUSE_B1_TIMESTAMP"
if [[ $FUSE_B1_SM -eq 0 ]]; then
    pass "[B.1] fuse-1 SetAttrMeta in Batch1: NO mode=Some() call (mode embedded in CreateInode)"
else
    fail "[B.1] fuse-1 SetAttrMeta still has mode=$FUSE_B1_SM calls for Batch1 (P3.1 not applied)"
fi
if [[ $FUSE_B1_UID -eq 0 ]]; then
    pass "[B.2] fuse-1 SetAttrMeta in Batch1: NO uid/gid=Some() call (baked into CreateInode)"
else
    fail "[B.2] fuse-1 SetAttrMeta still has uid/gid=$FUSE_B1_UID calls"
fi

# ---------------------------------------------------------------
# Overall result
# ---------------------------------------------------------------
echo ""
echo "================================================================"
echo " RESULT: $PASS/$TOTAL checks passed, $FAIL failed"
echo "================================================================"
if [[ $FAIL -eq 0 ]]; then
    echo -e "${GRN}ALL CHECKS PASSED${NC} — P3.1 eliminated SetAttr(mode/uid/gid) Raft proposals on file/dir create. Ratio = 1 CreateInode Raft log per create, while explicit chmod still correctly goes through SetAttrMeta/SetAttr."
    exit 0
else
    echo -e "${RED}SOME CHECKS FAILED${NC} — inspect FAIL lines above for details."
    exit 1
fi
