# powerfs-ctl

Declarative deployment and lifecycle control plane for PowerFS.

You edit one file — `cluster.toml` — and `powerfs-ctl` renders the compose
stack and per-service configs, issues mTLS certificates, brings services up
with raft health gating, and handles day-2 operations (rolling restart,
scaling masters/data nodes, cert renewal, diagnostics).

## Prerequisites

- Docker Engine with the `docker compose` plugin (v2)
- The PowerFS container image available to Docker:
  - default tag `ghcr.io/powerfs/powerfs:latest` (set `image_tag` in
    `cluster.toml` to use a local build, e.g. `powerfs:latest`)
- Outbound access from this host to the management network (master HTTP on
  port 9300, raft gRPC on 9335)
- A prepared kernel for the **kernel client** is handled separately via DKMS —
  see [../kernel/README.md](../kernel/README.md#dkms-install-auto-rebuild-on-kernel-upgrades)

## Build

```bash
# from repo root — build everything: the rendered compose bind-mounts the
# host's target/release/powerfs-{master,filer,volume,monitor,s3} binaries
# into the containers, so `-p powerfs-ctl` alone leaves bootstrap without
# the services it needs (ctl now preflight-checks this and errors out).
cargo build --release
# ctl binary: target/release/powerfs-ctl
```

All examples below assume the binary is on `PATH` (or prefix with
`cargo run -p powerfs-ctl --`).

## Quick start — one-command cluster

```bash
# 3-master HA cluster on 172.30.0.0/16 (the default profile)
powerfs-ctl bootstrap --profile ha

# single-node-ish simple profile, custom subnet
powerfs-ctl bootstrap --profile simple --network 10.10.0.0/16

# HA with 5 masters
powerfs-ctl bootstrap --profile ha --nodes 5
```

`bootstrap` is idempotent and performs, in order:

1. `init` — generate `.powerfs/cluster.toml` if missing
2. render `docker-compose.yml` + per-role TOML
3. start the master quorum and run the health gate (a zombie leader that
   cannot commit is detected *before* any cert is signed against it)
4. pull the cluster CA (`cert init-ca`)
5. issue node certs for every filer/volume, and client certs declared with an
   IP in `cluster.toml`
6. start the remaining services (redis/volume/filer/monitor/s3) and gate again

If a later step fails, masters are already up — rerun the single failed step
(the error message prints the exact retry command) and then `powerfs-ctl up`.

## The declarative model

```
cluster.toml (you edit)  ──config render──▶  .powerfs/rendered/  ──up──▶  containers
                                              docker-compose.yml
                                              config/master-*.toml ...
```

- `powerfs-ctl init` writes `cluster.toml` once; afterwards **you own the
  file** — re-running `init`/`bootstrap` with different flags prints a warning
  and keeps your edits. To change topology, edit counts/IPs in `cluster.toml`,
  then run `powerfs-ctl up` (rendering happens automatically when the file is
  newer than the rendered compose).
- State directory: `./.powerfs` by default; override with `--home <dir>` or
  `POWERFS_HOME=<dir>` (global flag, works on every subcommand).

```
.powerfs/
├── cluster.toml          # source of truth (user-edited)
├── rendered/
│   ├── docker-compose.yml
│   └── config/           # per-service TOML consumed by containers
├── certs/                # ca.crt + issued node/client cert/key pairs
└── state.json
```

## Step-by-step bring-up (equivalent to bootstrap)

```bash
powerfs-ctl init                     # write .powerfs/cluster.toml
$EDITOR .powerfs/cluster.toml        # tune counts, tokens, image tag
powerfs-ctl config check             # validate + IP allocation preview
powerfs-ctl config show              # preview rendered output without writing
powerfs-ctl up --role master         # masters first, with health gate
powerfs-ctl cert init-ca             # fetch CA cert to .powerfs/certs/
powerfs-ctl cert issue filer-1 --san-ip 172.30.0.31 --node
powerfs-ctl cert issue volume-1 --san-ip 172.30.0.21 --node
powerfs-ctl up                       # everything else
powerfs-ctl status
```

## Day-2 operations

### Status, logs, diagnostics

```bash
powerfs-ctl status                  # raft leadership, quorum health, containers
powerfs-ctl doctor                  # read-only checks (Ok/Warn/Error grading)
powerfs-ctl doctor --fix            # also auto-fix low-risk issues (re-render)
powerfs-ctl logs --role master -f   # aggregated compose logs
powerfs-ctl restart --role master   # follower-first rolling restart, leader
                                    # last, full health gate after each node
powerfs-ctl restart --force         # skip gates between nodes
powerfs-ctl down                    # stop all services
powerfs-ctl down --purge            # also remove data volumes
```

`up`/`down`/`restart`/`logs` accept `--role master|volume|filer|redis|monitor|s3`
or a single service name such as `--role master-2`.

### Adding a master (scale the raft quorum)

Declare the new node first, then run one command:

```bash
# 1. increment count under [nodes.master] in .powerfs/cluster.toml
# 2. provision (render → cert → start container → wait for boot →
#    raft add-voter → full-quorum health gate)
powerfs-ctl node master add 4

# raft gRPC addr is auto-derived as <allocated-ip>:9335 from cluster.toml;
# override only if necessary
powerfs-ctl node master add 4 --addr 10.0.0.4:9335

# if the container is already running and bootstrapped, join raft only
powerfs-ctl node master add 4 --raft-only

powerfs-ctl node master list                 # membership + current leader
powerfs-ctl node master remove 2             # refuses to remove the leader / last voter
powerfs-ctl node master remove 1 --force     # bypass the leader guard
```

### Data nodes (volume / filer)

```bash
# add [nodes.volume] count in cluster.toml first, then:
powerfs-ctl node data add volume-7
powerfs-ctl node data add filer-4
# render → cert → compose up; data nodes auto-register via registration_token,
# no raft join required

powerfs-ctl node data maintenance volume-1       # drain / maintenance ON
powerfs-ctl node data maintenance volume-1 --off # back to service
powerfs-ctl node data remove volume-2
powerfs-ctl node data remove volume-2 --force    # even if it owns routes
```

### Certificates

```bash
powerfs-ctl cert list                          # registry + expiry from master
powerfs-ctl cert renew fuse-client-3           # re-sign, old cert kept (grace window)
powerfs-ctl cert renew fuse-client-3 --revoke-old
powerfs-ctl cert revoke --client-name fuse-client-3
powerfs-ctl cert revoke --fingerprint <sha256>
# issue a node cert (empty mount_dirs) or a client cert (scoped mount dirs)
powerfs-ctl cert issue kernel-node7 --san-ip 172.30.0.77 --node
powerfs-ctl cert issue fuse-ws1 --san-ip 172.30.0.99 \
    --mount-dir /data/powerfs
```

### Enrolling a client machine

```bash
powerfs-ctl client enroll fuse-ws1 --ip 172.30.0.99 --kind fuse
powerfs-ctl client enroll node7 --ip 172.30.0.77 --kind kernel
```

Issues a client certificate and renders the client-side config (client.toml /
fuse config). Kernel clients also need the module installed on that host —
see the [kernel DKMS guide](../kernel/README.md#dkms-install-auto-rebuild-on-kernel-upgrades).

## Command reference

| Command | Purpose |
|---|---|
| `bootstrap` | One-shot init + render + cert + up + health gate |
| `init` | Generate cluster.toml skeleton (`--force` to overwrite) |
| `up [--role R]` | render-if-stale + compose up + health gate |
| `down [--purge] [--role R]` | Stop (optionally wipe volumes) |
| `restart [--role R] [--force]` | Rolling restart, follower-first, gated |
| `status` | Cluster/quorum/container overview |
| `config render\|check\|show` | Render, validate, or preview configs |
| `cert init-ca` | Pull cluster CA into the state dir |
| `cert issue` | Issue a node/client cert |
| `cert list` | List registry entries with expiry |
| `cert renew` | Re-sign a client cert (`--revoke-old`) |
| `cert revoke` | Revoke by `--client-name` or `--fingerprint` |
| `node master add\|remove\|list` | Raft membership lifecycle |
| `node data add\|maintenance\|remove` | Volume/filer lifecycle |
| `client enroll` | Issue cert + render config for a new client |
| `doctor [--fix]` | Automated diagnostics |
| `logs [--role R] [-f]` | Aggregated service logs |

Run any command with `--help` for full flag reference.

## Notes on safety

- Mutating raft/admin calls target the current **leader** automatically
  (discovered via metrics polling); followers return a 503 with a leader hint
  and the client retries once at the new leader.
- Health gates verify one stable leader, `commit_index > 0`,
  `last_applied == commit_index`, and a stable term across two polls —
  split-brain and zombie leaders fail the gate instead of being silently
  accepted.
- The host running 5.15-era kernels cannot load the 6.17-only kernel module;
  `powerfs-ctl` itself manages user-space/container services only.
