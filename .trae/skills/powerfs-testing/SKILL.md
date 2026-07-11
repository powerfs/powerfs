---
name: "powerfs-testing"
description: "Manages PowerFS testing environment including cluster startup, FUSE client management, and test execution. Invoke when user wants to test PowerFS, run tests, or verify functionality."
---

# PowerFS Testing Skill

## Overview

This skill provides standardized procedures for testing PowerFS, including:
- Cluster environment management
- FUSE client operations
- Test execution
- Verification procedures

## 1. Environment Management

### Start Cluster

```bash
docker/scripts/start-cluster.sh [--build]
```

**IMPORTANT**: Always use the existing `start-cluster.sh` script. Do NOT create new docker-compose files!

### Start FUSE

```bash
docker/scripts/start-fuse.sh [--build]
```

**IMPORTANT**: FUSE must be started AFTER the cluster is ready.

### Stop Cluster

```bash
docker/scripts/stop-cluster.sh
```

## 2. Test Execution

### Unit Tests

```bash
cargo test --workspace
```

### Integration Tests

Run tests INSIDE FUSE container:

```bash
# RFS tester tests
docker exec fuse-1 /app/target/debug/deps/rfs_tester_fuse_test-xxx

# Volume verification tests
docker exec fuse-1 /app/target/debug/deps/volume_verification_test-xxx
```

### Manual Verification

```bash
# Basic write/read test
docker exec fuse-1 bash -c "echo 'test' > /mnt/powerfs/test.txt && cat /mnt/powerfs/test.txt"

# Cross-client consistency
docker exec fuse-1 bash -c "echo 'shared' > /mnt/powerfs/shared.txt"
docker exec fuse-2 cat /mnt/powerfs/shared.txt

# Persistence test
docker exec fuse-1 bash -c "echo 'persistent' > /mnt/powerfs/persist.txt"
docker compose restart fuse-1
docker exec fuse-1 cat /mnt/powerfs/persist.txt
```

## 3. Key Rules

### NEVER DO THESE

1. **Do NOT create new docker-compose files** - Use the existing `docker/docker-compose.yml`
2. **Do NOT run tests on host** - Run tests inside FUSE container
3. **Do NOT skip Volume verification** - Always verify data is written to Volume
4. **Do NOT ignore content_size=0** - This indicates data was NOT properly persisted

### ALWAYS DO THESE

1. **Always start cluster first** - FUSE depends on Master and Volume
2. **Always build binaries before testing** - `cargo build --release`
3. **Always verify after writing** - Read back data to confirm
4. **Always check logs** - Look for errors and `content_size` values

## 4. Common Issues

### Issue: File reads empty after write

**Symptom**: `cat /mnt/powerfs/test.txt` returns empty content

**Root Cause**: content_size not updated in metadata

**Solution**:
```bash
# Check FUSE logs
docker logs fuse-1 | grep "content_size"

# Rebuild with fix
cargo build --release --bin powerfs-fuse
docker cp target/release/powerfs-fuse fuse-1:/app/powerfs-fuse
docker compose restart fuse-1
```

### Issue: Port occupied

**Solution**:
```bash
docker/scripts/stop-cluster.sh
docker system prune -f
```

### Issue: FUSE container not running

**Solution**:
```bash
docker/scripts/stop-cluster.sh
docker/scripts/start-cluster.sh
docker/scripts/start-fuse.sh
```

## 5. Verification Checklist

Before declaring test success:

- [x] File write succeeds
- [x] File read returns correct content
- [x] content_size > 0 in logs
- [x] Data persists after FUSE restart
- [x] Cross-client consistency verified
- [x] No errors in FUSE/Master/Volume logs

## 6. References

- Testing Guidelines: `TESTING_GUIDELINES.md`
- Docker Compose: `docker/docker-compose.yml`
- Cluster Script: `docker/scripts/start-cluster.sh`
- FUSE Script: `docker/scripts/start-fuse.sh`
