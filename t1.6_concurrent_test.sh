#!/usr/bin/env bash
# T1.6 cross-client concurrent append test harness
set -u

TEST_NAME=${1:-c1_inline}
A_LINES=${2:-200}
B_LINES=${3:-200}
LINE_PAY=${4:-48}
(( LINE_TOTAL = LINE_PAY + 1 ))

# Container mount points — both point to the same PowerFS filesystem
CON1=fuse-1
CON2=fuse-2
DIR1=/mnt/powerfs/t1_6_${TEST_NAME}   # fuse-1 mount path (/mnt/powerfs inside container)
DIR2=/mnt/fuse/t1_6_${TEST_NAME}      # fuse-2 mount path (/mnt/fuse inside container — see docker-compose)
FILE1=${DIR1}/concurrent_append.bin
FILE2=${DIR2}/concurrent_append.bin

echo "=================================================="
echo "  T1.6 test: ${TEST_NAME}"
echo "  Writer A (${CON1}): ${A_LINES} lines  Writer B (${CON2}): ${B_LINES} lines"
echo "  Line payload: ${LINE_PAY}B  (= ${LINE_TOTAL}B w/ newline)"
echo "  Expected total: $((A_LINES + B_LINES)) lines, " \
     "$(( (A_LINES + B_LINES) * LINE_TOTAL )) bytes"
echo "  CON1 dir=${DIR1}   CON2 dir=${DIR2}"
echo "=================================================="

# Step 0: clean slate via fuse-1 (CON1)
docker exec ${CON1} bash -c '
d="$1"; f="$2"
rm -rf "$d"
mkdir -p "$d"
# Use touch instead of python open() for reliability (quote escaping inside nested bash -c + python can fail)
touch "$f"
sync
ls -la "$f"
' _ "${DIR1}" "${FILE1}" 2>&1

# Step 0a: Let Step-0 Raft applies + Filer Invalidate notifications propagate.
# Also creates a cross-endian anchor file on CON1 and forces CON2 to stat it.
# The anchor-lookup MUST travel to the Filer, which returns an updated
# root dir_version → CON2 drops `dir_cache.complete=true` (otherwise the
# subsequent barrier loops forever on the local NegativeComplete shortcut
# and never asks the Filer, returning ENOENT despite the file existing).
sleep 1
docker exec ${CON1} bash -c 'echo ok > /mnt/powerfs/t1_6_anchor_$$.txt; sync' 2>/dev/null || true
sleep 1
docker exec ${CON2} bash -c '
# Force a real Filer RPC via any name that CON1 definitely created.
# Fallback: probe a well-known historical file; if that too hits NegativeComplete
# then just exhaust stat retries on the anchor name itself.
for probe in /mnt/fuse/t1_6_anchor_*.txt /mnt/fuse/test_simple.txt /mnt/fuse/sanity.txt; do
  stat "$probe" >/dev/null 2>&1 && break
done
' 2>/dev/null || true
# Drop anchor (best effort)
docker exec ${CON1} bash -c 'rm -f /mnt/powerfs/t1_6_anchor_*.txt' 2>/dev/null || true

# Step 0b: Cross-client visibility barrier.
# CON2 (fuse-2) must see the same directory/file before any writer starts.
# Aggressive approach: explicitly stat each path component from root to bust dentry cache.
echo ""
echo "Cross-client visibility barrier (CON2 must see file from CON1)..."
for round in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  # Force CON2 to re-lookup each ancestor: root → dir → file
  # This flushes any negative dentry cache that might have been created
  # before CON1's mkdir/touch propagated.
  RESULT=$(docker exec ${CON2} bash -c '
d="$1"; f="$2"
# Walk up path and force re-lookup of every component (stat each path)
p=""
IFS=/ read -ra PARTS <<< "$d"
for part in "${PARTS[@]}"; do
  [ -z "$part" ] && continue
  p="$p/$part"
  stat "$p" >/dev/null 2>&1 || true
  ls "$p" >/dev/null 2>&1 || true
done
stat "$f" >/dev/null 2>&1 || true
# Now check
if [ -d "$d" ] && [ -f "$f" ]; then
  s=$(stat -c "%s" "$f" 2>/dev/null)
  if [ "$s" = "0" ]; then echo OK; exit 0; fi
fi
sleep 0.5
# Double check after sleep
if [ -d "$d" ] && [ -f "$f" ]; then s=$(stat -c "%s" "$f" 2>/dev/null); [ "$s" = "0" ] && echo OK && exit 0; fi
echo NOT_YET
exit 1
' _ "${DIR2}" "${FILE2}" 2>&1 || true)
  if [ "${RESULT}" = "OK" ]; then
    echo "  Round ${round}: CON2 sees file — barrier PASSED"
    break
  fi
  echo "  Round ${round}: CON2 not yet visible (result: ${RESULT})"
  sleep 1
  # T1.6 barrier fix: stale dir_cache.complete=true traps NegativeComplete forever.
  # After round 5, hard-reset CON2's userspace FUSE daemon → drops all
  # in-memory caches (dir_cache, dentry leases, metadata_ttl projections)
  # so the next lookup actually RPCs the Filer instead of short-circuiting.
  if [ $round -eq 5 ]; then
    echo "  [barrier] hard-resetting CON2 (fuse-2) to clear stale dir_cache.complete ..."
    docker restart ${CON2} >/dev/null 2>&1 || true
    sleep 6
    echo "  [barrier] fuse-2 restarted, retrying visibility check"
  fi
  if [ $round -eq 14 ]; then
    echo "ERROR: cross-client visibility barrier FAILED after 15 rounds"
    echo "  Debug info: CON2 listing follows"
    echo "  --- dir(${DIR2}):"
    docker exec ${CON2} bash -c "ls -la '${DIR2}' 2>&1 || true"
    echo "  --- parent($(dirname ${DIR2})):"
    docker exec ${CON2} bash -c "ls -la '$(dirname ${DIR2})' 2>&1 | tail -15"
    echo "  --- stat file:"
    docker exec ${CON2} bash -c "stat '${FILE2}' 2>&1 || true"
    exit 3
  fi
done

# Step 1: build the per-client writer python script on the HOST, then cp into container.
build_writer_script() {
  local outfile=$1 wid=$2 lines=$3 target_file=$4
  local seed
  [ "${wid}" = "A" ] && seed=0xC0FFEE || seed=0xDEADBEEF
  cat > ${outfile} <<PYEOF
#!/usr/bin/env python3
import random, string, time, os, errno
LINES = ${lines}
PAY   = ${LINE_PAY}
WID   = "${wid}"
FILE  = "${target_file}"
rng = random.Random(${seed})

def open_with_retry(path, mode, tries=10, base_ms=50):
    for t in range(1, tries+1):
        try:
            return open(path, mode)
        except BlockingIOError:
            delay = base_ms * (2 ** (t-1)) / 1000.0
            if t == tries: raise
            time.sleep(min(delay, 2.0))
        except OSError as e:
            if e.errno == errno.EIO and t < tries:
                time.sleep(min(base_ms * (2**(t-1)) / 1000.0, 2.0))
            else:
                raise

def write_with_retry(fh, data, tries=8, base_ms=30):
    for t in range(1, tries+1):
        try:
            fh.write(data)
            return
        except BlockingIOError:
            delay = base_ms * (2 ** (t-1)) / 1000.0
            if t == tries: raise
            time.sleep(min(delay, 2.0))
        except OSError as e:
            if e.errno == errno.EIO and t < tries:
                time.sleep(min(base_ms * (2**(t-1)) / 1000.0, 2.0))
            else:
                raise

f = open_with_retry(FILE, 'a')
if hasattr(f, 'reconfigure'):
    try: f.reconfigure(line_buffering=True)
    except Exception: pass

lines_out = 0
last_err = None
try:
    for i in range(1, LINES+1):
        marker = ''.join(rng.choices(string.ascii_lowercase, k=14))
        header = f"{i:08d}:{WID}:{marker}|"
        pad_len = PAY - len(header)
        assert pad_len >= 0, f"payload too short pay={PAY} header={len(header)}"
        body = header + ('x' * pad_len)
        assert len(body) == PAY, f"body len {len(body)} vs {PAY}"
        ok = False
        for t in range(1, 10):
            try:
                write_with_retry(f, body + '\n')
                f.flush()
                ok = True
                break
            except BlockingIOError:
                time.sleep(min(0.05 * (2**(t-1)), 2.0))
            except OSError as e:
                last_err = e
                if e.errno in (errno.EIO, errno.ENOSPC) and t < 9:
                    time.sleep(min(0.05 * (2**(t-1)), 2.0))
                else:
                    raise
        if not ok:
            raise RuntimeError(f"line {i} write+flush exhausted retries: last_err={last_err!r}")
        lines_out = i
finally:
    for t in range(1, 6):
        try:
            f.close()
            break
        except OSError as e:
            if t == 5: raise
            time.sleep(0.05 * (2**(t-1)))
print(f"writer done WID={WID} lines_out={lines_out}/{LINES}")
PYEOF
}

TMPDIR=$(mktemp -d /tmp/t16_XXXXX)
PY_A=${TMPDIR}/wA.py
PY_B=${TMPDIR}/wB.py
build_writer_script ${PY_A} A ${A_LINES} ${FILE1}
build_writer_script ${PY_B} B ${B_LINES} ${FILE2}
chmod +x ${PY_A} ${PY_B}

CON_A=/tmp/t16_writerA.py
CON_B=/tmp/t16_writerB.py
docker cp ${PY_A} ${CON1}:${CON_A}
docker cp ${PY_B} ${CON2}:${CON_B}

echo ""
echo "Starting concurrent appenders (parallel docker exec)..."
t_start=$(date +%s)
( docker exec ${CON1} python3 ${CON_A} ) &
PID_A=$!
( docker exec ${CON2} python3 ${CON_B} ) &
PID_B=$!
wait ${PID_A}; RC_A=$?
wait ${PID_B}; RC_B=$?
t_end=$(date +%s)
echo "Writer A rc=${RC_A}, Writer B rc=${RC_B}, elapsed $((t_end-t_start))s"
if [ ${RC_A} -ne 0 ] || [ ${RC_B} -ne 0 ]; then
  echo "FAILED: writer(s) exited non-zero"
  exit 2
fi

sleep 2

# Step 3b (A7+fix verification barrier): Restart both FUSE clients to drop
# ALL in-memory metadata caches (entry cache, inline_buffers, dentry leases,
# open_count tracking, and especially the "Exclusive lease held, local cache
# authoritative" shortcut that causes fuse-1's verification reader to skip
# Filer refresh and read a STALE content_size — which caused the previous
# false-negative where fuse-1 saw size=9800 while fuse-2 correctly saw 19600.
#
# After restart both clients have a clean slate; reopen travels to the Filer
# and returns the authoritative size/chunks so verification reflects reality.
echo ""
echo "Verification barrier: restarting FUSE clients to drop all caches..."
docker restart ${CON1} >/dev/null 2>&1 || true
docker restart ${CON2} >/dev/null 2>&1 || true
sleep 6
echo "Verification barrier: clients restarted. Now verifying on BOTH clients."

# Step 4: build verification script on host, cp to CON1, then exec.
PY_VER=${TMPDIR}/verify.py
cat > ${PY_VER} <<'PYEOF'
import sys, os, collections, re
file, a_lines, b_lines, pay = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
tot = a_lines + b_lines
line_total = pay + 1

sz = os.stat(file).st_size
print(f"stat size:                  {sz}")
print(f"expected size:              {tot * line_total}")

with open(file, 'rb') as f:
    raw = f.read()
actual_nls = raw.count(b'\n')
print(f"actual newline count:       {actual_nls}")
print(f"expected lines:             {tot}")

lines = raw.split(b'\n')
if lines and lines[-1] == b'':
    lines = lines[:-1]
else:
    print("WARNING: trailing data without newline at EOF")

bad_len = 0
key_counts = collections.Counter()
a_count = 0
b_count = 0
a_seqs = set()
b_seqs = set()
pat = re.compile(rb'^(\d{8}):([AB]):([a-z]{14})\|(x*)$')

for idx, ln in enumerate(lines, 1):
    if len(ln) != pay:
        bad_len += 1
        if bad_len <= 5:
            print(f"  line #{idx} bad length: {len(ln)} vs exp {pay}: {ln[:60]!r}")
        continue
    m = pat.match(ln)
    if not m:
        if bad_len <= 10:
            print(f"  line #{idx} malformed: {ln[:60]!r}")
        bad_len += 1
        continue
    seq_s, wid, marker, pad = m.groups()
    key = (int(seq_s), wid.decode())
    key_counts[key] += 1
    if wid == b'A':
        a_count += 1
        a_seqs.add(int(seq_s))
    else:
        b_count += 1
        b_seqs.add(int(seq_s))

print()
print("--- Per-writer counts ---")
print(f"  A lines in file: {a_count}  (expected {a_lines})")
print(f"  B lines in file: {b_count}  (expected {b_lines})")

print()
print("--- Unique (seq,wid) keys ---")
unique_keys = len(key_counts)
dup_keys = sum(1 for k,v in key_counts.items() if v > 1)
missing_a = [s for s in range(1,a_lines+1) if s not in a_seqs]
missing_b = [s for s in range(1,b_lines+1) if s not in b_seqs]
extra_a = [s for s in a_seqs if not (1 <= s <= a_lines)]
extra_b = [s for s in b_seqs if not (1 <= s <= b_lines)]
print(f"  unique keys:        {unique_keys} / {tot}")
print(f"  duplicated keys:    {dup_keys}")
print(f"  missing A seq:      {len(missing_a)}  (sample {missing_a[:10]})")
print(f"  missing B seq:      {len(missing_b)}  (sample {missing_b[:10]})")
print(f"  extra/unexpected A: {len(extra_a)}")
print(f"  extra/unexpected B: {len(extra_b)}")
print(f"  lines w/ bad format:{bad_len}")

problems = []
if sz != tot * line_total: problems.append(f"size mismatch {sz} vs {tot*line_total}")
if actual_nls != tot:    problems.append(f"newline count {actual_nls} vs {tot}")
if a_count != a_lines:     problems.append(f"A lines {a_count} vs {a_lines}")
if b_count != b_lines:     problems.append(f"B lines {b_count} vs {b_lines}")
if unique_keys != tot:     problems.append(f"unique keys {unique_keys} vs {tot}")
if dup_keys > 0:           problems.append(f"{dup_keys} duplicated keys")
if missing_a or missing_b: problems.append(f"missing seqs")
if bad_len > 0:            problems.append(f"{bad_len} malformed lines")

print()
if not problems:
    print("=== ALL VERIFICATIONS PASSED ===")
    with open(file, 'r') as f:
        lns = f.readlines()
    print("First 3 lines:")
    for l in lns[:3]: print(" ", l.rstrip())
    print("Last 3 lines:")
    for l in lns[-3:]: print(" ", l.rstrip())
else:
    print(f"=== VERIFICATION FAILED ({len(problems)} problems):")
    for p in problems: print("  -", p)
    with open(file, 'rb') as f:
        data = f.read()
    print(f"Head 300 bytes: {data[:300]!r}")
    print(f"Tail 300 bytes: {data[-300:]!r}")
    sys.exit(1)
PYEOF

echo ""
echo "--- Running verification on ${CON1} (file=${FILE1}) ---"
VERIFY_CON=/tmp/t16_verify.py
docker cp ${PY_VER} ${CON1}:${VERIFY_CON}
docker exec ${CON1} python3 ${VERIFY_CON} "${FILE1}" ${A_LINES} ${B_LINES} ${LINE_PAY}
ver_rc_1=$?

echo ""
echo "--- Running verification on ${CON2} (file=${FILE2}) ---"
docker cp ${PY_VER} ${CON2}:${VERIFY_CON}
docker exec ${CON2} python3 ${VERIFY_CON} "${FILE2}" ${A_LINES} ${B_LINES} ${LINE_PAY}
ver_rc_2=$?

echo ""
echo "=== Verification summary ==="
echo "  fuse-1 (${CON1}): exit=${ver_rc_1}"
echo "  fuse-2 (${CON2}): exit=${ver_rc_2}"

final_rc=0
if [ ${ver_rc_1} -ne 0 ] || [ ${ver_rc_2} -ne 0 ]; then
  final_rc=1
fi

echo ""
echo "Final test exit: ${final_rc}"
exit ${final_rc}
