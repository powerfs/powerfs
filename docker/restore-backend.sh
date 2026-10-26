#!/usr/bin/env bash
# Restore the canonical PowerFS backend on this host from the declarative
# single source of truth: $POWERFS_HOME/cluster.toml -> powerfs-ctl bootstrap.
#
# This is THE recovery path for the CI runner backend (referenced by
# .github/workflows/kernel-multi-client.yml) and for humans after debugging:
#
#   docker/restore-backend.sh
#
# What it does:
#   1. guards against trampling a foreign cluster (any running *-master-1
#      container that is not ours aborts the script)
#   2. removes a stale legacy `docker_powerfs-network` if it sits unused on
#      the powerfs-br0 bridge (legacy compose and rendered compose share the
#      bridge name; they cannot coexist)
#   3. seeds the master CA dir (<home>/rendered/certs) from the tracked
#      docker/certs-default fixture when missing or mismatched, so kernel/fuse
#      client certs from certs-default stay valid (CI mounts them into VMs)
#   4. runs `powerfs-ctl bootstrap` (idempotent: render -> master quorum ->
#      init-ca -> issue node+declared client certs -> full up -> health gate)
#   5. starts declared fuse client containers (cluster.toml [client.*] with
#      type="fuse" and an ip) via docker/run-fuse-client.sh
#   6. verifies the master container is up and its CA matches certs-default
#
# Env overrides:
#   POWERFS_HOME   state dir (default: <repo>/.powerfs)
#   SKIP_FUSE=1    do not start fuse client containers
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POWERFS_HOME="${POWERFS_HOME:-$REPO_ROOT/.powerfs}"
CTL="$REPO_ROOT/target/release/powerfs-ctl"
CERTS_DEFAULT="$REPO_ROOT/docker/certs-default"

die() { echo "ERROR: $*" >&2; exit 1; }

[ -x "$CTL" ] || die "$CTL missing — build first: cargo build --release -p powerfs-ctl"
[ -f "$POWERFS_HOME/cluster.toml" ] || \
    die "$POWERFS_HOME/cluster.toml not found — run: $CTL --home $POWERFS_HOME init"
[ -f "$CERTS_DEFAULT/ca.crt" ] || die "$CERTS_DEFAULT/ca.crt missing (tracked fixture)"

CLUSTER_NAME=$(sed -n 's/^[[:space:]]*name[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' \
    "$POWERFS_HOME/cluster.toml" | head -1)
[ -n "$CLUSTER_NAME" ] || die "cannot parse cluster name from $POWERFS_HOME/cluster.toml"
MASTER_CTNR="${CLUSTER_NAME}-master-1"
echo "cluster: $CLUSTER_NAME (home: $POWERFS_HOME)"

# --- 1. foreign-cluster guard -------------------------------------------------
foreign=$(docker ps --format '{{.Names}}' | grep -E '(^|-)master-1$' | grep -vx "$MASTER_CTNR" || true)
[ -z "$foreign" ] || die "foreign master container(s) running: $foreign
  Another cluster occupies this host. Tear it down first (or debug it), then re-run."

# --- 2. stale legacy network --------------------------------------------------
# The legacy docker/docker-compose.yml project ('docker') and the rendered
# project both bind com.docker.network.bridge.name=powerfs-br0. If the legacy
# network survived a `compose down`, remove it when unused so the rendered
# network can take the bridge. `network rm` fails while endpoints exist, which
# is exactly the case we must not touch.
if docker network ls --format '{{.Name}}' | grep -qx 'docker_powerfs-network'; then
    if [ -z "$(docker network inspect -f '{{len .Containers}}' docker_powerfs-network)" ] || \
       [ "$(docker network inspect -f '{{len .Containers}}' docker_powerfs-network)" = "0" ]; then
        echo "removing stale legacy network docker_powerfs-network (unused)"
        docker network rm docker_powerfs-network >/dev/null
    else
        die "legacy network docker_powerfs-network still has containers attached"
    fi
fi

# --- 3. seed master CA from certs-default -------------------------------------
SEED_DIR="$POWERFS_HOME/rendered/certs"
mkdir -p "$SEED_DIR"
want_fp=$(openssl x509 -in "$CERTS_DEFAULT/ca.crt" -noout -fingerprint -sha256 | cut -d= -f2)
got_fp=""
[ -f "$SEED_DIR/ca.crt" ] && \
    got_fp=$(openssl x509 -in "$SEED_DIR/ca.crt" -noout -fingerprint -sha256 | cut -d= -f2)
if [ "$got_fp" != "$want_fp" ]; then
    echo "seeding master CA from docker/certs-default (was: ${got_fp:-none})"
    # Master containers run as root and own previously generated CA files.
    # Re-own the dir so the copy below (and future re-seeds) need no sudo.
    [ -w "$SEED_DIR" ] || sudo chown "$(id -u):$(id -g)" "$SEED_DIR"
    rm -f "$SEED_DIR/ca.crt" "$SEED_DIR/ca.key" "$SEED_DIR/client_registry.json"
    cp "$CERTS_DEFAULT/ca.crt" "$CERTS_DEFAULT/ca.key" \
       "$CERTS_DEFAULT/client_registry.json" "$SEED_DIR/"
    chmod 600 "$SEED_DIR/ca.key"
fi

# --- 4. bootstrap (idempotent) -------------------------------------------------
POWERFS_HOME="$POWERFS_HOME" "$CTL" bootstrap

# --- 5. fuse client containers --------------------------------------------------
# Parse declared fuse clients with an ip: lines after a `[client.<name>]`
# header, until the next header. Only type="fuse" clients with ip= are started.
if [ "${SKIP_FUSE:-0}" != "1" ]; then
    awk '
        /^\[client\.[^]]+\]/ {
            if (name != "" && typ == "fuse" && ip != "") print name, ip
            name = $0; sub(/^\[client\./, "", name); sub(/\].*$/, "", name)
            typ = ""; ip = ""; next
        }
        /^[[:space:]]*type[[:space:]]*=/ {
            v = $0; sub(/^[^"]*"/, "", v); sub(/".*$/, "", v); typ = v; next
        }
        /^[[:space:]]*ip[[:space:]]*=/ {
            v = $0; sub(/^[^"]*"/, "", v); sub(/".*$/, "", v); ip = v; next
        }
        END { if (name != "" && typ == "fuse" && ip != "") print name, ip }
    ' "$POWERFS_HOME/cluster.toml" | while read -r fname fip; do
        echo "starting fuse client: $fname ($fip)"
        POWERFS_HOME="$POWERFS_HOME" CERTS_DIR="$POWERFS_HOME/certs" \
            "$REPO_ROOT/docker/run-fuse-client.sh" "$fname" "$fip"
    done
fi

# --- 6. verify ------------------------------------------------------------------
docker ps --format '{{.Names}}' | grep -qx "$MASTER_CTNR" || \
    die "$MASTER_CTNR not running after bootstrap"
ca_src=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data/master/ca"}}{{.Source}}{{end}}{{end}}' "$MASTER_CTNR")
[ -n "$ca_src" ] || die "cannot resolve /data/master/ca mount source of $MASTER_CTNR"
run_fp=$(openssl x509 -in "$ca_src/ca.crt" -noout -fingerprint -sha256 | cut -d= -f2)
[ "$run_fp" = "$want_fp" ] || die "running master CA ($run_fp) != certs-default ($want_fp)"

echo "✓ backend restored: $MASTER_CTNR healthy, CA matches docker/certs-default"
