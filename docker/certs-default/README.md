# PowerFS Default Dev/Test Certificate Bundle

This directory holds the **pre-shared default certificate bundle** checked
into the repository so that a fresh `docker compose up` works out of the
box for local development and testing.

## Contents (after running `scripts/generate-certs.sh`)

| File                       | Purpose                                                  |
| -------------------------- | -------------------------------------------------------- |
| `ca.crt`                   | Cluster CA certificate, distributed to every node       |
| `ca.key`                   | CA private key (master uses to sign leaf certs)         |
| `client_registry.json`     | Master-side registry of issued leaf cert fingerprints   |
| `filer-*.crt` / `*.key`    | Storage-node certs (filer-1/2/3)                        |
| `volume-server-*.crt/.key` | Storage-node certs (volume-server-1..6)                 |
| `fuse-client-*.crt/.key`   | FUSE user-space client certs (fuse-client-1/2)          |
| `kernel-client-*.crt/.key` | Kernel client certs (kernel-client-1/2 for VM mounts)   |

## How this directory is used

`docker-compose.yml` and `docker-compose.rdma.yml` mount this directory into
every service container:

- Master container:  `./certs-default` → `/data/master/ca`  (read-write,
  master loads `ca.crt`/`ca.key` and writes `client_registry.json` here)
- Filer / Volume / Fuse containers: `./certs-default` → `/etc/powerfs/certs`
  (read-only, used as `ca_crt` / `client_crt` / `client_key`)

So the directory is BOTH the master CA store AND the leaf-cert distribution
point for the dev/test cluster.

## Generating the default bundle

Run once after enabling `ca_dir` in `master-*.toml` and starting the master
container:

```bash
# 1. start master (auto-generates ca.crt + ca.key + client_registry.json
#    into this directory on first start)
docker compose -f docker/docker-compose.yml up -d master-1 master-2 master-3

# 2. sign all storage node + client certs (writes them into this directory)
./scripts/generate-certs.sh --topology three \
    --master-api 172.30.0.11:9300 \
    --output-dir docker/certs-default

# 3. commit the bundle
git add docker/certs-default/
git commit -m "chore(certs): add default dev/test certificate bundle"
```

For the single-node RDMA topology:

```bash
docker compose -f docker/docker-compose.rdma.yml up -d master-1
./scripts/generate-certs.sh --topology single \
    --master-api 192.168.100.3:9300 \
    --output-dir docker/certs-default
```

## Production deployment — REPLACE THIS BUNDLE

These certificates are PUBLIC (committed to the repository). Anyone with
repo access can impersonate any node. For production you MUST regenerate
certificates and mount `docker/certs/` instead of `docker/certs-default/`.

```bash
# 1. generate production certs into gitignored docker/certs/
./scripts/generate-certs.sh --topology three \
    --master-api <prod-master>:9300 \
    --admin-token <real-admin-token-from-master.toml> \
    --output-dir docker/certs

# 2. override the compose mount with an env var or edit compose file:
#    replace ./certs-default with ./certs in docker-compose.yml
#    (docker/certs/ is gitignored — secrets stay local)

# 3. start cluster with production certs
docker compose -f docker/docker-compose.yml up -d
```

## Replacing certs on a running cluster

1. Stop the affected service (e.g. `docker compose stop filer-1`)
2. Replace the cert files in `docker/certs/` (or `docker/certs-default/`)
3. Restart: `docker compose up -d filer-1`

For kernel clients (VM mounts), unmount first, replace files, then re-mount
with the new `ca_crt`/`client_crt`/`client_key` paths.

## Security notes

- `ca.key` in this directory is the master signing key for the dev/test
  cluster. Anyone with it can mint arbitrary node/client certs. Treat
  this bundle as TEST-ONLY.
- The `admin_token` in `master-*.toml` (`powerfs-admin-test`) is also
  public. Production deployments MUST change it.
- Cert expiry is 1 year (leaf) / 10 years (CA). Regenerate before expiry.
