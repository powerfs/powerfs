FROM ubuntu:20.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    libfuse3-3 \
    fuse3 \
    netcat-openbsd \
    libssl1.1 \
    fio \
    python3 \
    bc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /mnt/powerfs && chown root:root /mnt/powerfs && chmod 755 /mnt/powerfs
RUN mkdir -p /var/log && chown root:root /var/log && chmod 755 /var/log

# -----------------------------------------------------------------------------
# BUILD_BINS build-arg controls whether host-compiled Rust release binaries
# are shipped INSIDE the docker image at build time.
#
#   BUILD_BINS=0 (DEFAULT, recommended for day-to-day dev):
#       Docker image does NOT contain any of the 10 powerfs-<svc> binaries.
#       Each container MUST receive its binary through the bind-mounts
#       declared in docker-compose.yml (./target/release/powerfs-X:/app/powerfs-X:ro).
#       After every `cargo build --release` on the host, a
#       `docker-compose restart <svc>` is enough to pick up the new binary.
#       This eliminates the historic "stale baked-in binary" class of bugs
#       where someone rebuilds the Rust code, forgets to rebuild the docker
#       image, and then spends hours debugging why new endpoints 404, new
#       Raft ShardCommands are unknown, or metrics counters stay zero —
#       because the container was still running a weeks-old binary that
#       came from inside the image, overriding the newly-mapped volume.
#
#   BUILD_BINS=1 (opt-in for pre-baked CI/QA images only):
#       All 10 release binaries from ./target/release/ are copied into
#       /app/ inside the image and therefore survive without any bind
#       mount. This is useful for throwaway clusters where the host does
#       not have a Rust toolchain at all, OR for validating that a specific
#       image tag is fully self-contained. Caveat: every single Rust code
#       change then requires a full `docker build --build-arg BUILD_BINS=1`
#       cycle to propagate, so it is intentionally not the default.
# -----------------------------------------------------------------------------
ARG BUILD_BINS=0

# Stage binaries into a non-executable scratch folder first. If any binary
# is missing AND BUILD_BINS=1 we fail the build immediately so the user
# gets an actionable "re-run cargo build --release" error. If BUILD_BINS=0
# we simply discard the staged set (layer is still produced by COPY but
# the next RUN zeroes out /app before anyone can use it). This also keeps
# the layer-cache semantics sane: a new Rust build invalidates only this
# COPY layer and everything below it, not the apt-get layer above.
COPY target/release/powerfs-master /opt/bins-stash/powerfs-master
COPY target/release/powerfs-filer  /opt/bins-stash/powerfs-filer
COPY target/release/powerfs-s3     /opt/bins-stash/powerfs-s3
COPY target/release/powerfs-volume /opt/bins-stash/powerfs-volume
COPY target/release/powerfs-monitor /opt/bins-stash/powerfs-monitor
COPY target/release/powerfs-fuse   /opt/bins-stash/powerfs-fuse
COPY target/release/powerfs-init   /opt/bins-stash/powerfs-init
COPY target/release/powerfs-cli    /opt/bins-stash/powerfs-cli
COPY target/release/powerfs-testutil /opt/bins-stash/powerfs-testutil

COPY config/ /etc/powerfs/
COPY docker/ensure-bins-mounted.sh /app/ensure-bins-mounted.sh
RUN chmod 0755 /app/ensure-bins-mounted.sh

# Gate: install or discard staged binaries. chmod +x on the final /app copy
# so bind-mountless runs (BUILD_BINS=1) are immediately executable.
RUN set -eux; \
    if [ "${BUILD_BINS}" = "1" ]; then \
        for f in /opt/bins-stash/*; do \
            if [ ! -s "$f" ]; then \
                echo "ERROR: BUILD_BINS=1 but staged binary $f is empty or missing." >&2; \
                echo "       Re-run 'cargo build --release' before building this Dockerfile." >&2; \
                exit 1; \
            fi; \
        done; \
        cp -L /opt/bins-stash/* /app/; \
        chmod 0755 /app/powerfs-*; \
        echo "BUILD_BINS=1: $(ls /app/powerfs-* | wc -l) binaries embedded in image."; \
    else \
        rm -rf /opt/bins-stash; \
        echo "BUILD_BINS=0 (default): no binaries shipped in image — docker-compose bind-mount of target/release/ is required."; \
    fi

EXPOSE 9333 8080 8081 8888 8889 9000 8890

ENTRYPOINT ["/app/ensure-bins-mounted.sh"]
CMD ["/app/powerfs-master", "--config", "/etc/powerfs/master-1.toml"]
