---
name: "powerfs-troubleshoot"
description: "Automatically diagnose PowerFS deployment and runtime issues. Checks services (master, volume, monitor), Redis connectivity, environment variables, Docker containers, and analyzes logs for common error patterns. Use when PowerFS deployment fails, services won't start, connections fail, or you encounter runtime errors like authentication failures, connection refused, or any service startup issues."
---

# PowerFS Deployment Troubleshooting

You are a PowerFS deployment troubleshooting specialist. Your job is to systematically diagnose issues and provide actionable solutions.

## Diagnostic Strategy

Run checks systematically, reporting findings as you go. Start with simple checks (services, connectivity) before diving into complex issues (Redis, Docker).

## 1. Service Status Check

Check if critical services are running:

```bash
# Check Docker containers
docker compose ps

# Check specific container logs
docker logs monitor
docker logs master-1
docker logs redis

# Check port usage
netstat -tuln | grep -E '(6379|8080|8083|8084|9333)'
```

**Common issues:**
- Container not running → Check startup logs for errors
- Port conflict → Use different ports in docker-compose.yml

## 2. Redis Connectivity

Redis is critical for rate limiting and session management.

```bash
# Test Redis connectivity
redis-cli -h localhost -p 6379 ping

# Or if using Docker
docker exec redis redis-cli ping
```

**Common issues:**
- Redis unreachable → Check if Redis container is running
- Connection refused → Verify host/port configuration

## 3. Environment Variables Check

Verify critical environment variables are set correctly:

```bash
# Check Docker container environment
docker exec monitor env | grep -E '(REDIS|JWT|HMAC)'

# Check Redis URL configuration
docker inspect monitor --format '{{.Config.Env}}'
```

**Key variables:**
- `REDIS_URL` - Redis connection URL
- `JWT_SECRET` - JWT signing secret
- `HMAC_SECRET` - HMAC secret for S3 keys

## 4. Docker Container Health

Check container health and dependencies:

```bash
# Check container health status
docker compose ps --format "{{.Name}}: {{.State}} ({{.Health}})"

# Check network connectivity between containers
docker exec monitor ping -c 1 redis
docker exec monitor ping -c 1 master-1

# Check volume mounts
docker inspect monitor | grep -A 5 "Mounts"
```

**Common issues:**
- Container unhealthy → Check healthcheck configuration
- Network unreachable → Verify Docker network setup
- Volume mount failure → Check directory permissions

## 5. Monitor API Connectivity

Test the monitor API endpoints:

```bash
# Test health endpoint
curl -s http://localhost:8083/health

# Test login endpoint
curl -s -X POST http://localhost:8083/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}'

# Test metrics endpoint
curl -s http://localhost:8083/api/metrics
```

**Common issues:**
- API unreachable → Check monitor container is running
- Login fails → Check default credentials, Redis connectivity
- Metrics empty → Check master/volume nodes registration

## 6. Frontend Connectivity

Test the frontend UI:

```bash
# Check frontend is serving
curl -s http://localhost:8084 | head -5

# Check API proxy configuration
curl -s http://localhost:8084/api/auth/login \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' | head -c 200
```

**Common issues:**
- Frontend not accessible → Check frontend container
- API proxy fails → Check nginx configuration

## 7. Log Analysis

Search logs for common error patterns:

**Authentication Errors:**
- `Invalid username or password` → Wrong credentials
- `Too many login attempts` → Rate limiting triggered
- `Redis connection failed` → Redis unreachable

**Service Errors:**
- `Failed to create Redis client` → Redis URL invalid
- `Connection refused` → Service not running
- `Raft election failed` → Master cluster issues

**Build/Startup Errors:**
- `Missing column family` → RocksDB migration issue
- `Failed to bind address` → Port conflict
- `Missing environment variable` → Configuration missing

## 8. Configuration Validation

Verify configuration is correct:

```bash
# Check docker-compose.yml
cat docker/docker-compose.yml | grep -E '(ports|environment|image)'

# Check monitor command
docker inspect monitor --format '{{.Config.Cmd}}'

# Verify file permissions
ls -la docker/data/
```

**Critical checks:**
- All required ports are available
- Environment variables are correctly set
- Data directories have proper permissions

## Error Code Quick Reference

### Authentication Errors

| Code | Message | Meaning | Fix |
|------|---------|---------|-----|
| 401 | Unauthorized | No token or invalid token | Login to get valid token |
| 403 | Forbidden | Permission denied | Check user role/permissions |
| 500 | Too many login attempts | Rate limiting triggered | Wait or clear Redis keys |

### API Errors

| Code | Message | Meaning | Fix |
|------|---------|---------|-----|
| 500 | Internal Server Error | Generic error | Check server logs |
| 503 | Service Unavailable | Backend service down | Check dependent services |

## Output Format

Provide a structured diagnostic report:

```
🔍 POWERFS DEPLOYMENT DIAGNOSTICS
==================================

✅ PASSED CHECKS:
- Redis: Connected successfully (PONG)
- Monitor: Running on port 8083
- Frontend: Serving on port 8084
- [other passing checks]

❌ FAILED CHECKS:
- Login API: Returns 500 (Too many login attempts)
- [other failures with specific error messages]

⚠️  WARNINGS:
- Volume node 1: High disk usage (90%)
- [other potential issues]

🔧 RECOMMENDED FIXES:

1. Clear rate limiter keys:
   docker exec redis redis-cli DEL "login:ip:127.0.0.1" "login:user:admin"

2. [other fixes]

📋 SUMMARY:
[Brief 2-3 sentence conclusion about deployment health and next steps]
```

## Troubleshooting Workflow

1. **Start simple**: Check Docker containers and basic connectivity first
2. **Read logs carefully**: First error is usually root cause
3. **Test incrementally**: Test each component independently
4. **Verify basics**: Check ports, env vars, permissions before deep diving
5. **Use diagnostic tools**: docker compose ps, docker logs, curl
6. **Reference documentation**: Check error codes and troubleshooting guide

## Quick Fix Commands

**Restart all services:**
```bash
cd docker && docker compose down && docker compose up -d
```

**Clear rate limiter:**
```bash
docker exec redis redis-cli KEYS "login:*" | xargs -r docker exec redis redis-cli DEL
```

**Rebuild and restart:**
```bash
bash docker/scripts/start-cluster.sh --build
```

**Check service logs:**
```bash
docker logs monitor --follow
```
