#!/bin/bash
# Run/stop a FUSE client container for a powerfs-ctl bootstrapped cluster.
#
# The rendered compose has no fuse services — clients are enrolled first
# (signs the cert + writes client-<name>.toml), then launched here:
#
#   ./target/release/powerfs-ctl client enroll fuse-1 --ip 172.30.0.41 --kind fuse
#   docker/run-fuse-client.sh fuse-1 172.30.0.41
#   docker/run-fuse-client.sh stop fuse-1
#
# Access the mount INSIDE the container (`docker exec <name> ls /mnt/powerfs`):
# the host bind dir /tmp/powerfs/<name> is rprivate and does NOT track the
# fuse view (same as the fuse services in docker/docker-compose.yml).
#
# Env overrides: NETWORK (default rendered_powerfs-network),
# CERTS_DIR (default <repo>/.powerfs/certs), IMAGE (default powerfs:latest)
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NETWORK="${NETWORK:-rendered_powerfs-network}"
CERTS_DIR="${CERTS_DIR:-$REPO_ROOT/.powerfs/certs}"
IMAGE="${IMAGE:-powerfs:latest}"
FUSE_BIN="$REPO_ROOT/target/release/powerfs-fuse"

if [ "$1" = "stop" ]; then
    [ -n "$2" ] || { echo "usage: $0 stop <name>"; exit 1; }
    docker rm -f "$2"
    exit 0
fi

NAME="$1"; IP="$2"
[ -n "$NAME" ] && [ -n "$IP" ] || { echo "usage: $0 <name> <ip> | $0 stop <name>"; exit 1; }

for f in ca.crt "$NAME.crt" "$NAME.key" "client-$NAME.toml"; do
    [ -f "$CERTS_DIR/$f" ] || {
        echo "ERROR: $CERTS_DIR/$f missing — run first:"
        echo "  $REPO_ROOT/target/release/powerfs-ctl client enroll $NAME --ip $IP --kind fuse"
        exit 1
    }
done
[ -x "$FUSE_BIN" ] || { echo "ERROR: $FUSE_BIN missing — cargo build --release"; exit 1; }
docker network inspect "$NETWORK" >/dev/null 2>&1 || {
    echo "ERROR: docker network '$NETWORK' not found — run bootstrap first"
    exit 1
}

mkdir -p "/tmp/powerfs/$NAME"
docker rm -f "$NAME" 2>/dev/null || true

# Flags mirror the fuse services in docker/docker-compose.yml (privileged,
# /dev/fuse, host mount dir), but certs/config come from the enroll output
# and cert paths are passed on the CLI — the rendered client toml has no
# tls keys.
docker run -d --name "$NAME" --hostname "$NAME" --init --privileged \
    --network "$NETWORK" --ip "$IP" \
    --device /dev/fuse \
    --cap-add SYS_ADMIN --cap-add DAC_READ_SEARCH \
    --security-opt apparmor:unconfined \
    -v "/tmp/powerfs/$NAME:/mnt/powerfs" \
    -v "$CERTS_DIR:/etc/powerfs/certs:ro" \
    -v "$CERTS_DIR/client-$NAME.toml:/app/config/fuse.toml:ro" \
    -v "$FUSE_BIN:/app/powerfs-fuse:ro" \
    "$IMAGE" \
    /app/powerfs-fuse --config /app/config/fuse.toml --verbose --container \
        --ca-crt /etc/powerfs/certs/ca.crt \
        --client-crt "/etc/powerfs/certs/$NAME.crt" \
        --client-key "/etc/powerfs/certs/$NAME.key"

echo "$NAME up — access via: docker exec $NAME ls /mnt/powerfs"
