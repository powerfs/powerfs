#!/bin/bash
# =============================================================================
# PowerFS single-container demo entrypoint.
#
# Brings up, in order: redis → master (leader gate) → filer + volume
# (port gate + settle) → fuse mount → end-to-end write/read selftest.
# Everything talks over 127.0.0.1 inside this one container.
#
# DEMO ONLY: the CA key baked under /opt/demo/ca is a public, throwaway
# secret. This image must never be used to hold real data.
# =============================================================================
set -euo pipefail

CERTS_BAKED=/opt/demo/ca
CERTS_LIVE=/etc/powerfs/certs
DATA_MASTER_CA=/data/master/ca
LOGDIR=/var/log/powerfs
mkdir -p "$LOGDIR" /data/master /data/filer /data/volume /data/redis /mnt/powerfs

log()  { echo -e "\033[1;36m[demo]\033[0m $*"; }
fail() { echo -e "\033[1;31m[demo FATAL]\033[0m $*" >&2; tail -n 20 "$LOGDIR"/*.log 2>/dev/null | tail -40 >&2 || true; exit 1; }

# Allow `docker run --rm -it powerfs/demo bash` for debugging.
if [ "$#" -gt 0 ]; then
    exec "$@"
fi

# ---------------------------------------------------------------------------
# First-boot: the master loads ca.crt/ca.key/client_registry.json from its
# ca_dir, which lives on the data volume. Seed it from the baked bundle.
# ---------------------------------------------------------------------------
if [ ! -f "$DATA_MASTER_CA/ca.crt" ]; then
    log "first boot — seeding master CA from baked demo bundle"
    mkdir -p "$DATA_MASTER_CA"
    cp "$CERTS_BAKED"/{ca.crt,ca.key,client_registry.json} "$DATA_MASTER_CA"/
    chmod 600 "$DATA_MASTER_CA/ca.key"
fi

wait_tcp() { # host port timeout_sec
    local host=$1 port=$2
    local timeout=${3:-30}
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then exec 3<&- 3>&-; return 0; fi
        sleep 1
    done
    return 1
}

PIDS=()
start_svc() { # name cmd...
    local name=$1; shift
    log "starting $name"
    ("$@") >>"$LOGDIR/$name.log" 2>&1 &
    PIDS+=($!)
}

cleanup() {
    log "shutting down"
    if [ "${#PIDS[@]}" -gt 0 ]; then
        for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
        sleep 1
        for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
    fi
}
trap cleanup EXIT INT TERM

# ---- redis (all services require redis_url at startup) ----
log "starting redis"
redis-server --bind 127.0.0.1 --port 6379 --daemonize yes \
    --dir /data/redis --logfile "$LOGDIR/redis.log" \
    --save "" --appendonly no
wait_tcp 127.0.0.1 6379 15 || fail "redis did not open :6379"

# ---- master: gate on single-node raft becoming leader ----
start_svc master /app/powerfs-master --config /etc/powerfs/master.toml
deadline=$((SECONDS + 60))
until [ "$SECONDS" -ge "$deadline" ]; do
    if curl -sf --max-time 2 http://127.0.0.1:9300/metrics 2>/dev/null \
        | awk '$1=="powerfs_is_leader"{exit ($2==1)?0:1}'; then
        log "master is raft leader"; break
    fi
    sleep 1
done
[ "$SECONDS" -lt "$deadline" ] || fail "master did not become leader within 60s"

# ---- filer + volume: self-register, filer self-formats on empty data dir ----
start_svc filer /app/powerfs-filer --config /etc/powerfs/filer.toml
start_svc volume /app/powerfs-volume --config /etc/powerfs/volume.toml
wait_tcp 127.0.0.1 8888 30 || fail "filer did not open :8888"
wait_tcp 127.0.0.1 8091 30 || fail "volume did not open :8091"
log "filer/volume listening — waiting for registration + root inode bootstrap"
sleep 10

# ---- fuse mount (requires --device /dev/fuse + SYS_ADMIN) ----
if [ ! -e /dev/fuse ]; then
    fail "/dev/fuse missing — run with: --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined"
fi
start_svc fuse /app/powerfs-fuse --config /etc/powerfs/fuse.toml
deadline=$((SECONDS + 45))
until grep -q " /mnt/powerfs " /proc/mounts 2>/dev/null; do
    [ "$SECONDS" -lt "$deadline" ] || fail "fuse mount did not appear within 45s"
    sleep 1
done
log "fuse mounted at /mnt/powerfs"

# ---- end-to-end readiness proof: write through fuse, read back, delete ----
deadline=$((SECONDS + 40))
ok=0
while [ "$SECONDS" -lt "$deadline" ]; do
    if echo "powerfs-demo-$RANDOM" > /mnt/powerfs/.demo_selftest \
       && [ -s /mnt/powerfs/.demo_selftest ] \
       && rm -f /mnt/powerfs/.demo_selftest; then ok=1; break; fi
    sleep 2
done
[ "$ok" = 1 ] || fail "fuse write/read selftest failed — see $LOGDIR/fuse.log"

cat <<'BANNER'

============================================================
  PowerFS demo cluster is READY
  ---------------------------------------------------------
  Try it:
    docker exec -it <container> bash
    ls /mnt/powerfs            # shared fuse filesystem
    echo hi > /mnt/powerfs/a   # create a file

  DEMO ONLY: single-node, loopback mTLS, throwaway baked CA.
  Not for production. Data lives under /data (ephemeral
  unless -v powerfs-demo-data:/data is given).
============================================================
BANNER

# Stay foreground; exit if any service dies.
wait -n
fail "a background service exited unexpectedly"
