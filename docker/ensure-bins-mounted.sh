#!/usr/bin/env bash
# =============================================================================
# ensure-bins-mounted.sh — entrypoint safety wrapper for powerfs service images.
#
# The default docker image (BUILD_BINS=0) ships ZERO Rust binaries inside the
# image itself — they are expected to arrive at runtime via the bind-mounts
# declared in docker-compose.yml:
#
#     volumes:
#       - ./target/release/powerfs-filer:/app/powerfs-filer:ro
#       - ./config:/etc/powerfs:ro
#
# If a developer mistakenly (a) runs `docker run` without the bind-mount,
# (b) restarts a container whose volume line was deleted from compose, or
# (c) forgets `cargo build --release` such that the host path is empty,
# exec'ing CMD directly would print a generic "exec /app/powerfs-xxx: no
# such file or directory" and exit 127 — very unhelpful.
#
# This wrapper instead:
#   1. Inspects the first positional argument (the binary CMD points to).
#   2. Verifies that it exists AND is a non-empty regular file.
#   3. If NOT, emits a clear, actionable error that:
#        - names the missing binary,
#        - points to the docker-compose volume-mount convention,
#        - reminds the user of the BUILD_BINS=1 escape hatch to bake
#          binaries into the image instead, and
#        - exits with code 2 (ENOENT semantics, easy to grep for).
#   4. Otherwise: `exec`s the binary with all remaining arguments.
#
# Environment knobs:
#   POWERFS_BIN_CHECK=0 — skip the presence check entirely (do NOT set this
#       in CI; only for ad-hoc interactive shells inside the container).
# =============================================================================
set -u

BIN="${1:-}"

# --- 0. Allow interactive shell / diagnostic invocations without a binary ---
if [ -z "${BIN}" ] \
   || [ "${BIN}" = "sh" ] \
   || [ "${BIN}" = "bash" ] \
   || [ "${BIN}" = "/bin/sh" ] \
   || [ "${BIN}" = "/bin/bash" ] \
   || [ "${BIN}" = "sleep" ] \
   || [ "${BIN}" = "true" ] \
   || [ "${BIN}" = "false" ]; then
    # Passthrough without any check — this keeps `docker run ... bash`
    # usable for debugging even on the BUILD_BINS=0 image.
    exec "$@"
fi

# --- 1. Optional opt-out escape hatch (not recommended in CI) -------------
if [ "${POWERFS_BIN_CHECK:-1}" = "0" ]; then
    exec "$@"
fi

# --- 2. Fail loudly if binary is missing -----------------------------------
if [ ! -e "${BIN}" ]; then
    cat >&2 <<'EOF'
===============================================================================
  [powerfs] ENTRYPOINT ERROR — binary not found in container filesystem.
===============================================================================
  Expected binary:
EOF
    echo "    ${BIN}" >&2
    cat >&2 <<'EOF'

  The image was built with BUILD_BINS=0 (the default), so no powerfs
  binaries are baked into the image. Docker-compose MUST bind-mount
  ./target/release/powerfs-<svc> from your host source tree onto
  /app/powerfs-<svc>:ro inside the container, e.g.

      services:
        filer-1:
          image: powerfs:latest
          volumes:
            - ./target/release/powerfs-filer:/app/powerfs-filer:ro
            - ./config:/etc/powerfs:ro

  First thing to try:
      cargo build --release
      # (re)start containers so the new binary is bind-mounted:
      docker-compose -f docker/docker-compose.yml restart

  Alternative — bake binaries INTO the image (slower, CI/packaging only):
      docker build \
          --build-arg BUILD_BINS=1 \
          -f docker/Dockerfile \
          -t powerfs:baked .

  (If you wanted an interactive shell, run `docker run ... bash`; that
   bypasses this check on purpose.)
===============================================================================
EOF
    exit 2
fi

if [ ! -s "${BIN}" ]; then
    cat >&2 <<EOF
===============================================================================
  [powerfs] ENTRYPOINT ERROR — binary exists but is EMPTY:
    ${BIN}
  This almost always means your host-side cargo build --release failed or
  did not produce this binary. Re-run cargo build --release and verify
  target/release/$(basename "${BIN}") is a non-zero ELF, then restart.
  (BUILD_BINS=1 image build will also catch this at docker-build time.)
===============================================================================
EOF
    exit 2
fi

# --- 3. All good. Exec the binary — ENTRYPOINT + CMD args are preserved. ---
exec "$@"
