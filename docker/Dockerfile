FROM ubuntu:20.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    libfuse3-3 \
    fuse3 \
    netcat-openbsd \
    libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /mnt/powerfs && chown root:root /mnt/powerfs && chmod 755 /mnt/powerfs
RUN mkdir -p /var/log && chown root:root /var/log && chmod 755 /var/log

COPY target/release/powerfs /app/
COPY target/release/powerfs-volume /app/
COPY target/release/powerfs-monitor /app/
COPY target/release/powerfs-fuse /app/
COPY target/release/powerfs-filer /app/
COPY config/ /etc/powerfs/

EXPOSE 9333 8080 8081 8888 8889

CMD ["/app/powerfs", "master", "--config", "/etc/powerfs/master-1.toml"]