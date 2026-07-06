FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    libfuse3-3 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY target/release/powerfs /app/
COPY target/release/powerfs-volume /app/
COPY target/release/powerfs-monitor /app/

EXPOSE 9333 8080 8081

CMD ["/app/powerfs", "master", "--port", "9333", "--dir", "/data/master"]