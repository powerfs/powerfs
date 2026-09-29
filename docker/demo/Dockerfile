# PowerFS single-container demo image.
#
# Self-contained trial environment: redis + 1 master + 1 filer + 1 volume +
# one fuse mount, all over 127.0.0.1. Certificates are minted at image build
# time by powerfs-ctl (fresh throwaway CA) — nothing is bind-mounted.
#
# Build context MUST be the repo root (needs target/release binaries):
#   docker build -f docker/demo/Dockerfile -t powerfs-demo:dev .
# Run (Linux host with FUSE):
#   docker run -d --name powerfs --device /dev/fuse \
#     --cap-add SYS_ADMIN --security-opt apparmor:unconfined powerfs-demo:dev
#   docker exec -it powerfs bash
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    libfuse3-3 \
    fuse3 \
    libssl1.1 \
    redis-server \
    # powerfs-master links libibverbs unconditionally (RDMA-capable build)
    libibverbs1 \
    librdmacm1 \
    ibverbs-providers \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Host-built release binaries (build script runs cargo first).
COPY target/release/powerfs-master /app/powerfs-master
COPY target/release/powerfs-filer  /app/powerfs-filer
COPY target/release/powerfs-volume /app/powerfs-volume
COPY target/release/powerfs-fuse   /app/powerfs-fuse
COPY target/release/powerfs-ctl    /app/powerfs-ctl
RUN for f in /app/powerfs-*; do [ -s "$f" ] || { echo "ERROR: $f empty/missing — run docker/demo/build.sh" >&2; exit 1; }; chmod 0755 /app/powerfs-*; done

# Static loopback configs.
COPY docker/demo/config/ /etc/powerfs/
RUN mkdir -p /etc/powerfs/certs

# Fresh demo CA + node/client certs + registry, generated offline in-layer.
# The CA private key ends up in the image by design (trial only).
RUN /app/powerfs-ctl cert demo-bundle --dir /opt/demo/ca \
 && cp /opt/demo/ca/ca.crt \
       /opt/demo/ca/filer-1.crt /opt/demo/ca/filer-1.key \
       /opt/demo/ca/volume-1.crt /opt/demo/ca/volume-1.key \
       /opt/demo/ca/fuse-demo.crt /opt/demo/ca/fuse-demo.key \
       /etc/powerfs/certs/

RUN mkdir -p /mnt/powerfs /data && chmod 755 /mnt/powerfs
VOLUME ["/data"]

COPY docker/demo/entrypoint.sh /usr/local/bin/powerfs-demo-entrypoint.sh
RUN chmod 0755 /usr/local/bin/powerfs-demo-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/powerfs-demo-entrypoint.sh"]
