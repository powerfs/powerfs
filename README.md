# README.md

PowerFS
**Unified Volume Engine for HPC, AI & Cloud Native Storage**

*One storage layer, three protocols - Files, KV Cache, S3 Objects*

[Introduction](#introduction) • [Architecture](#architecture) • [Core Features](#core-features) • [Roadmap](#roadmap) • [Scenarios](#scenarios) • [Benchmark](#benchmark) • [License](#license)



---

## Introduction

**PowerFS** is a next-generation unified storage engine built from scratch with Rust, centered around a **unified volume management layer**. Unlike traditional storage solutions that use raw Block devices or OSD (Object Storage Device), PowerFS abstracts storage into logical volumes with a protocol-agnostic Needle-format data engine, enabling a single storage layer to support three access protocols: POSIX files, LLM KV tensor cache, and S3 objects.

### Why Unified Volume?

The unified volume approach offers significant advantages over traditional Block and OSD models:

| Feature | Block Storage | OSD (Ceph) | Unified Volume (PowerFS) |
|---------|--------------|------------|--------------------------|
| **Data Abstraction** | Raw blocks | Objects | **Logical volumes** |
| **Indexing** | None | Object ID | **O(1) Needle indexing** |
| **Multi-Protocol Support** | Requires file system | Object-only | **Files, KV, S3 natively** |
| **Erasure Coding** | External required | Separate component | **Built-in integration** |
| **Bitrot Detection** | None | Configurable | **Built-in integration** |
| **Cross-Protocol Sharing** | Not supported | Requires RGW | **Native support** |

### Architecture Overview

PowerFS innovates a **unified volume engine architecture** that builds three independent services on top of a single data layer:
- **File Service**: Unified Volume + Directory Service + Distributed Lock
- **KV Cache Service**: Unified Volume + Session Consistency Management
- **S3 Object Service**: Unified Volume + S3 Protocol + Object Consistency Management

This approach solves the fragmentation problem of separated HPC and AI storage systems, delivering ultra-low latency, zero-jitter performance for converged HPC simulation and LLM AI cluster workloads.

---

## Core Design Philosophy

- **Unified Volume Engine**: A single data layer (Needle format) supporting Files, KV Cache and S3 Objects with O(1) constant-time addressing

- **Protocol-Agnostic Storage**: The volume layer is protocol-agnostic; protocol-specific consistency management is added at the service layer

- **Zero-Jitter Priority**: Foreground computing I/O is prioritized; background balancing, GC and encoding tasks are fully noise-reduced to ensure steady-state performance

- **Full Hardware Offloading**: Native adaptation to SPDK, RDMA and GPU Direct, end-to-end zero-copy hardware acceleration

- **Lightweight Enterprise-Grade**: Simplified architecture, linear horizontal scaling, low operation and maintenance costs, enterprise-level high availability and fault tolerance

---

## Core Features

### ⚡ Extreme HPC Parallel Capability

- Distributed sharded metadata architecture, supporting 10,000+ MPI process concurrent read and write

- Complete standard POSIX semantics, fully compatible with mainstream HPC simulation software and parallel computing frameworks

- Adaptive file striping and multi-node aggregated I/O, supporting PB-level cluster aggregated bandwidth

- Fine-grained job-level QoS and I/O isolation, eliminating resource preemption and ensuring zero-jitter steady-state operation

- Optimized ultra-large directory and massive small-file scenarios, solving traditional HPC storage small-file performance bottlenecks

### 🧠 Native LLM KV Cache Engine (Industry Exclusive)

- Built-in dedicated KV tensor storage engine, no third-party components, deeply optimized for LLM inference characteristics

- O(1) constant-time KV addressing, microsecond-level access latency, supporting incremental update and partial overwriting

- Dual elimination strategy of LRU hot and cold sorting + TTL session expiration, realizing intelligent cache automatic management

- Session-level cache isolation and hot data resident mechanism, greatly improving long-text inference token generation throughput

- Native GPU Direct zero-copy transmission, extending GPU HBM video memory with NVMe storage to completely solve LLM inference video memory bottlenecks

### 🗄️ Native S3 Object Storage

- Built-in S3-compatible object storage interface, fully compatible with AWS S3 protocol and SDK

- Unified metadata management by Master node, data stored distributedly on Volume Server nodes

- Support for bucket operations, object CRUD, multipart upload, versioning and lifecycle management

- Native integration with PowerFS distributed storage engine, no additional middleware required

- Compatible with mainstream S3 tools (AWS CLI, s3cmd, S3 Browser)

### 🚀 Ultra-Low Latency Hardware Acceleration

- SPDK user-state NVMe bare disk I/O, bypassing kernel file system and system call overhead, maximizing hardware IOPS and bandwidth

- Full-link RDMA lossless network instead of TCP, eliminating network soft interrupts and kernel protocol stack overhead

- Dual-client mode: lightweight FUSE user client + high-performance Linux kernel client

- No periodic jitter caused by runtime GC, stable p99/p999 latency under full-load cluster

### 🛠 Lightweight & Highly Available OPS

- Stateless master scheduling cluster based on Raft consensus, no single point of failure, unlimited horizontal scaling

- Rack-aware topology scheduling, realizing local I/O and intelligent data load balancing

- Dual storage engine of multi-replica & EC erasure coding, adaptive hot and cold data hierarchical storage

- Automatic node/disk fault detection, data migration and cluster self-healing

- Simplified deployment and operation, significantly lower maintenance costs than traditional Lustre/BeeGFS

---

## Architecture

PowerFS adopts a **unified volume engine architecture** with three-layer decoupling, realizing complete separation of control plane and data plane. The core is the Unified Volume Layer that abstracts storage into logical volumes with a single Needle-format data engine, supporting three access protocols through protocol-specific consistency management:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Layer 3: Multi-Protocol Access Layer                    │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────┐  │
│  │     FUSE Client      │  │     S3 Gateway       │  │   KV Cache       │  │
│  │  (POSIX / FUSE)      │  │  (HTTP/REST)         │  │  (gRPC)          │  │
│  │  ┌────────────────┐  │  │  ┌────────────────┐  │  │  ┌──────────────┐ │  │
│  │  │ PowerFsFs      │  │  │  │ HTTP Server    │  │  │  │ Session Mgmt │ │  │
│  │  │ (FUSE Layer)   │  │  │  │ (9000)         │  │  │  │ LRU Cache   │ │  │
│  │  └────────┬───────┘  │  │  │ Auth Manager   │  │  │  │ GPU Direct  │ │  │
│  │  │ MetaCache       │  │  │  │ MultiPart Mgr  │  │  │  └──────────────┘ │  │
│  │  │ (Metadata Cache)│  │  │  └────────┬───────┘  │  │                   │  │
│  │  └────────┬───────┘  │  │          │            │  │                   │  │
│  │  │ ChunkCache      │  │  │          │            │  │                   │  │
│  │  │ (Chunk Cache)   │  │  │          │            │  │                   │  │
│  │  └────────┬───────┘  │  │          │            │  │                   │  │
│  └───────────┼──────────┘  └──────────┼────────────┘  └─────────┬──────────┘  │
│              │                       │                        │             │
│              └───────────────────────┼────────────────────────┘             │
│                                      │                                     │
└──────────────────────────────────────▼──────────────────────────────────────┘
                                      │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                    Layer 2: Control Plane (Master Layer)                   │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    Master Raft Cluster (3 nodes)                     │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                     │   │
│  │  │ Master-1   │  │ Master-2   │  │ Master-3   │                     │   │
│  │  │ (Leader)   │  │(Follower)  │  │(Follower)  │                     │   │
│  │  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘                     │   │
│  │        │              │              │                              │   │
│  │        └──────────────┼──────────────┘                              │   │
│  │                       ▼                                             │   │
│  │  ┌──────────────────────────────────────────────────────────────┐   │   │
│  │  │              DirectoryTree (Unified Metadata)                │   │   │
│  │  │  - POSIX File Metadata (inode, dentry, attributes)           │   │   │
│  │  │  - S3 Bucket/Object Metadata (path→FID mapping)              │   │   │
│  │  │  - KV Cache Session Metadata                                 │   │   │
│  │  └──────────────────────────────────────────────────────────────┘   │   │
│  │  ┌──────────────────────────────────────────────────────────────┐   │   │
│  │  │              LockManager (Distributed Lock)                  │   │   │
│  │  │  - Leader Local Lock (<1μs)                                  │   │   │
│  │  │  - Raft Lease Lock (~10ms)                                   │   │   │
│  │  └──────────────────────────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                  Layer 1: Unified Volume Layer                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  Volume 1    │  │  Volume 2    │  │  Volume 3    │  │  Volume N    │    │
│  │  (8080)      │  │  (8081)      │  │  (8082)      │  │  (8xxx)      │    │
│  │  ┌──────────┐│  │  ┌──────────┐│  │  ┌──────────┐│  │  ┌──────────┐│    │
│  │  │ Needle   ││  │  │ Needle   ││  │  │ Needle   ││  │  │ Needle   ││    │
│  │  │ Engine   ││  │  │ Engine   ││  │  │ Engine   ││  │  │ Engine   ││    │
│  │  └──────────┘│  │  └──────────┘│  │  └──────────┘│  │  └──────────┘│    │
│  │  ┌──────────┐│  │  ┌──────────┐│  │  ┌──────────┐│  │  ┌──────────┐│    │
│  │  │ EC       ││  │  │ EC       ││  │  │ EC       ││  │  │ EC       ││    │
│  │  │ Coding   ││  │  │ Coding   ││  │  │ Coding   ││  │  │ Coding   ││    │
│  │  └──────────┘│  │  └──────────┘│  │  └──────────┘│  │  └──────────┘│    │
│  │  ┌──────────┐│  │  ┌──────────┐│  │  ┌──────────┐│  │  ┌──────────┐│    │
│  │  │ Bitrot   ││  │  │ Bitrot   ││  │  │ Bitrot   ││  │  │ Bitrot   ││    │
│  │  │ Detection││  │  │ Detection││  │  │ Detection││  │  │ Detection││    │
│  │  └──────────┘│  │  └──────────┘│  │  └──────────┘│  │  └──────────┘│    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Layer 1: Unified Volume Layer (Core)

The core of PowerFS - a protocol-agnostic unified data engine:

- **Distributed Volume Server nodes** for actual data storage, abstracted as logical volumes
- **Single Needle-format data engine** supporting Files, KV Cache and S3 Objects with O(1) constant-time addressing
- **Multi-replica and EC erasure coding** for data reliability
- **Bitrot detection, recycle bin, WORM locking** and automatic data repair capabilities
- **Protocol-agnostic design** - the volume layer doesn't know whether data comes from FUSE, KV or S3

### Layer 2: Control Plane (Master Layer)

- **High-availability Raft master cluster** for cluster topology management, resource allocation and task scheduling
- **Unified metadata management (DirectoryTree)** including POSIX file metadata, S3 bucket/object metadata and KV cache session metadata
- **Volume allocation and mapping**, routing data requests to appropriate Volume Server nodes
- **Distributed lock service (three-tier lock model)** ensuring data consistency across protocols
- **Protocol-specific consistency management**: Directory Service + Lock for Files; Session Management for KV; Object Versioning for S3

### Layer 3: Multi-Protocol Access Layer

Three types of access interfaces built on top of the unified volume layer with protocol-specific logic:

| Interface | Protocol | Use Case | Protocol-Specific Logic |
|-----------|----------|----------|------------------------|
| **FUSE** | POSIX | HPC parallel computing, traditional file operations | Directory Service + Distributed Lock |
| **S3** | HTTP/REST | Cloud-native object storage, AI dataset storage | S3 Protocol + Object Consistency |
| **KV Cache** | gRPC | LLM inference KV tensor cache | Session Isolation + GPU Direct |

---

## Roadmap


---

## Application Scenarios

- **HPC Supercomputing Cluster**: Fluid mechanics, meteorological simulation, structural calculation, material simulation and large-scale MPI parallel computing jobs

- **AI Training Cluster**: Massive dataset storage, large model training high-throughput reading and writing, model file persistent storage

- **LLM Inference Cluster**: Long-text dialogue KV cache acceleration, GPU video memory overflow solution, high-concurrency inference service optimization

- **Cloud-Native Storage**: S3-compatible object storage for cloud-native applications, containerized workloads

- **HPC & AI Converged Cluster**: Unified storage resource pooling, isolated coexistence of supercomputing and intelligent computing workloads

---

## Benchmark

### FIO Performance Test Results

All tests are conducted on a single-node setup with PowerFS FUSE client, using standard `fio` benchmark tool.

#### Test Environment
- **Hardware**: Single node with NVMe SSD
- **Block Size**: 4KB (random), 1MB (sequential)
- **Test Size**: 100MB per test (50MB per thread for multi-thread tests)
- **IO Engine**: `sync` (standard POSIX I/O)

#### Maximum Performance (Async Mode - Cached Writes)

| Test Type | Block Size | IOPS | Bandwidth | Avg Latency |
|-----------|------------|------|-----------|-------------|
| Sequential Write | 1MB | 5,556 | 5.4 GiB/s | 161 usec |
| Sequential Read | 1MB | 492 | 493 MiB/s | 2,019 usec |
| Random Write | 4KB | 610,000 | 2.3 GiB/s | 1.3 usec |
| Random Read | 4KB | 7,178 | 28.0 MiB/s | 138 usec |
| Mixed Read/Write (70%/30%) | 4KB | 13,472 | 52.6 MiB/s | - |

#### Persistent Performance (Sync Mode - With fsync)

| Test Type | Block Size | IOPS | Bandwidth | Avg Latency | fsync Latency |
|-----------|------------|------|-----------|-------------|--------------|
| Sequential Write | 1MB | 213 | 214 MiB/s | 460 usec | 3,279 usec |
| Sequential Read | 1MB | 480 | 481 MiB/s | 2,072 usec | - |
| Random Write | 4KB | 770 | 3.1 MiB/s | 10 usec | 1,279 usec |
| Random Read | 4KB | 7,184 | 28.1 MiB/s | 138 usec | - |
| Mixed Read/Write (70%/30%) | 4KB | 1,605 | 6.3 MiB/s | - | 643 usec |

#### Multi-thread Performance (4 Threads)

| Test Type | Block Size | IOPS | Bandwidth | Avg Latency |
|-----------|------------|------|-----------|-------------|
| Sequential Write (async) | 1MB | 12,500 | 12.2 GiB/s | 251 usec |
| Sequential Write (fsync) | 1MB | 365 | 366 MiB/s | 516 usec |
| Random Read | 4KB | 23,300 | 91.1 MiB/s | 169 usec |

#### Test Commands

```bash
# Run full benchmark suite with default settings (async mode)
bash scripts/run_fio_test.sh

# Run with persistent writes (fsync=1)
bash scripts/run_fio_test.sh --force-fsync

# Run with custom IO engine
bash scripts/run_fio_test.sh --engine=libaio
bash scripts/run_fio_test.sh --engine=io_uring

# Custom fsync interval (every 1000 I/Os)
bash scripts/run_fio_test.sh --engine=libaio --fsync=1000
```

#### Test Script Options

| Option | Description |
|--------|-------------|
| `--engine=ENGINE` | IO engine: `sync` (default), `libaio`, `io_uring` |
| `--fsync=N` | Number of I/Os between fsync (0=disabled, 1=every IO) |
| `--no-fsync` | Shortcut for `--fsync=0` (cached writes) |
| `--force-fsync` | Shortcut for `--fsync=1` (persistent writes) |
| `--no-build` | Skip building release binaries |

#### Key Insights

- **Maximum Write Performance**: Random writes reach 610K IOPS (2.3 GiB/s) with cached writes, demonstrating excellent write buffer efficiency
- **Multi-thread Scaling**: 4-thread sequential write reaches 12.5K IOPS (12.2 GiB/s), showing effective parallel processing
- **Persistent Performance**: Limited by gRPC round-trip and disk fsync (~1.3ms), typical for network-attached storage
- **Read Performance**: Random reads reach 23.3K IOPS with 4 threads, primarily limited by disk I/O
- **Performance Gap**: Async mode shows ~800x faster random write throughput compared to sync mode (610K vs 770 IOPS)

### Benchmark Outlook

PowerFS targets leading performance among mainstream open-source distributed storage systems, with core advantages as follows:

- **vs General Cloud-Native Storage**: Higher parallel computing concurrency, lower steady-state jitter, native KV cache AI acceleration capability

- **vs Traditional HPC File System**: Lighter architecture, lower O&M cost, better small-file performance, natively adapted to AI inference scenarios and S3 object storage

- **vs Lightweight Distributed Storage**: Complete POSIX HPC semantics, enterprise-level high availability and QoS isolation, professional supercomputing cluster carrying capacity

---

## Getting Started

### Prerequisites

- Rust 1.70+ (with cargo)
- Protobuf compiler (`protoc`)
- FUSE development libraries (for FUSE client)
- Linux kernel headers (for FUSE)

#### Ubuntu/Debian

```bash
sudo apt-get update && sudo apt-get install -y \
    protobuf-compiler \
    libfuse-dev \
    linux-headers-generic
```

#### CentOS/RHEL

```bash
sudo yum install -y \
    protobuf-compiler \
    fuse-devel
```

### Build

```bash
# Clone the repository
git clone https://github.com/powerfs/powerfs.git
cd powerfs

# Build all packages
cargo build --all

# Build in release mode
cargo build --all --release

# Build and install to PATH
cargo install --path powerfs-server
```

### Quick Start

Run PowerFS similar to SeaweedFS:

```bash
# Step 1: Start master node (default port 9333)
powerfs master

# Step 2: Start volume node connected to master
powerfs volume -m localhost:9333

# Step 3: Start filer (REST API, default port 8888)
powerfs filer -m localhost:9333

# Step 4: Mount FUSE filesystem
powerfs mount -d /mnt/powerfs -m localhost:9333

# Step 5: Start S3 backend (default port 9000)
powerfs s3 --master localhost:9333
```

### Run

#### Start Master Node

```bash
# Start master node with default settings
powerfs master

# Start with custom port and directory
powerfs master -p 9333 -d /data/master

# Separate raft and meta directories (for production)
powerfs master -d /data/master -r /fast-ssd/raft -m /fast-ssd/meta

# Bind to specific IP
powerfs master -p 9333 -i 192.168.1.100
```

#### Start Volume Node

```bash
# Start volume node with minimal configuration
powerfs volume -m localhost:9333

# Start with custom settings
powerfs volume -p 8080 -d /data/volume -m localhost:9333

# Separate meta and data directories (for production)
# Meta on fast SSD, data on large capacity disk
powerfs volume -d /data/vol1 \
    -m /fast-ssd/vol1/meta \
    -d /big-disk/vol1/data \
    -m localhost:9333

# Bind to specific IP with custom max volume size
powerfs volume -p 8080 -i 192.168.1.101 -d /data/volume -m localhost:9333 -s 2147483648
```

#### Start Filer

```bash
# Start filer connected to master
powerfs filer -m localhost:9333

# Start with custom port
powerfs filer -p 8888 -m localhost:9333

# Bind to specific IP
powerfs filer -p 8888 -i 192.168.1.100 -m localhost:9333
```

#### Start S3 Backend

```bash
# Start S3 backend with minimal configuration
powerfs s3 --master localhost:9333

# Start with custom port
powerfs s3 --port 9000 --master localhost:9333

# Start with custom access keys
powerfs s3 --port 9000 --master localhost:9333 \
  --access-key myaccesskey --secret-key mysecretkey

# Bind to specific IP
powerfs s3 --port 9000 --ip 192.168.1.100 --master localhost:9333
```

#### Mount FUSE Filesystem

```bash
# Mount PowerFS to /mnt/powerfs
powerfs mount -d /mnt/powerfs

# Mount with master connection
powerfs mount -d /mnt/powerfs -m localhost:9333

# Alternative: use fuse command
powerfs fuse -d /mnt/powerfs -m localhost:9333
```

### Directory Structure

PowerFS uses a hierarchical directory structure to separate different types of data:

```
# Master Node Directory Structure
/data/master/
├── raft/           # Raft consensus log (can be on fast SSD)
│   ├── wal/        # Write-Ahead Log
│   └── snapshot/   # State snapshots
└── meta/           # RocksDB metadata (cluster topology, volume mapping)
    └── *.sst        # RocksDB SST files

# Volume Node Directory Structure
/data/volume/
├── meta/           # RocksDB metadata (volume info, needle index)
│   └── *.sst       # RocksDB SST files
└── data/           # Actual file data (can be on large capacity disk)
    └── volume_{id}/
        ├── data    # Volume data file
        └── index   # Volume index
```

**Directory Separation Benefits:**
- Place raft logs on fast SSD for better consensus performance
- Place metadata on fast SSD for quick lookups
- Place data files on large capacity disks

### Command Line Options

```bash
PowerFS - Zero-jitter unified parallel file system

Usage: powerfs [OPTIONS] <COMMAND>

Commands:
  master   Start master node (port: 9333, dir: ./data/master)
  volume   Start volume node (port: 8080, dir: ./data/volume)
  filer    Start filer (REST API, port: 8888)
  s3       Start S3 backend (port: 9000, dir: ./data/s3)
  fuse     Mount FUSE filesystem
  mount    Mount filesystem (alias for fuse)
  help     Print this message or the help of the given subcommand(s)

Options:
      --log-level <LOG_LEVEL>  Log level [default: info]
  -h, --help                   Print help
  -V, --version                Print version

Master Options:
  -p, --port <PORT>    Master port [default: 9333]
  -d, --dir <DIR>      Data directory [default: ./data/master]
  -r, --raft-dir       Raft log directory [default: <dir>/raft]
  -m, --meta-dir       Meta storage directory [default: <dir>/meta]
  -i, --ip <IP>        Bind IP address

Volume Options:
  -p, --port <PORT>           Volume port [default: 8080]
  -d, --dir <DIR>             Data directory [default: ./data/volume]
  -m, --meta-dir              Meta storage directory [default: <dir>/meta]
  -d, --data-dir              Data storage directory [default: <dir>/data]
      --master <MASTER>       Master address
  -i, --ip <IP>               Bind IP address
  -s, --max-volume-size <MAX_VOLUME_SIZE>  Max volume size in bytes [default: 1073741824]

Filer Options:
  -p, --port <PORT>    Filer port [default: 8888]
      --master <MASTER>  Master address
  -i, --ip <IP>        Bind IP address

S3 Backend Options:
  -p, --port <PORT>       S3 port [default: 9000]
      --master <MASTER>   Master address
  -i, --ip <IP>           Bind IP address
      --access-key        S3 access key [default: powerfs]
      --secret-key        S3 secret key [default: powerfs123]

Mount/Fuse Options:
  -d, --dir <DIR>      Mount directory
      --master <MASTER>  Master address
```

### Run Tests

```bash
# Run all tests
cargo test --all

# Run tests for specific package
cargo test -p powerfs-core
```

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Multi-Protocol Access Layer                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  ┌─────────────────────┐  │
│  │  FUSE    │  │  Filer   │  │   KV Cache      │  │   S3 Clients        │  │
│  │  Mount   │  │  (HTTP)  │  │  (LLM Inference)│  │  (AWS CLI, SDK...)  │  │
│  └────┬─────┘  └────┬─────┘  └───────┬──────────┘  └───────────┬─────────┘  │
└───────┼──────────────┼───────────────┼───────────────────────────┼───────────┘
        │              │               │                           │
        └──────────────┼───────────────┼───────────────────────────┘
                       │               │
┌───────────────────────▼───────────────▼──────────────────────────────────────┐
│                    Master Layer (Raft Consensus Cluster)                    │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Cluster Management | Resource Allocation | Metadata Management     │   │
│  │  - DirectoryTree (POSIX file metadata)                              │   │
│  │  - S3 Bucket/Object metadata                                        │   │
│  │  - Volume allocation & mapping                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼──────────────────────────────────────────────┐
│                         Unified Volume Layer                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  Volume 1    │  │  Volume 2    │  │  Volume 3    │  │  Volume N    │    │
│  │  (8080)      │  │  (8081)      │  │  (8082)      │  │  (8xxx)      │    │
│  │  - File Data │  │  - File Data │  │  - File Data │  │  - File Data │    │
│  │  - KV Cache  │  │  - KV Cache  │  │  - KV Cache  │  │  - KV Cache  │    │
│  │  - EC Coding │  │  - EC Coding │  │  - EC Coding │  │  - EC Coding │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
```

### S3 Architecture

PowerFS implements a native S3-compatible object storage interface:

| Component | Port | Role |
|-----------|------|------|
| **PowerFS Master** | 9333 | Metadata management (buckets, objects), volume allocation |
| **PowerFS Volume Server** | 8080+ | Actual data storage for S3 objects |
| **PowerFS S3 Gateway** | 9000 | S3 API frontend, routes requests to Master and Volume Servers |

**Data Flow:**
1. S3 client sends request to S3 Gateway
2. S3 Gateway queries Master for metadata (bucket/object info)
3. Master returns FID (File ID) and Volume Server location
4. S3 Gateway reads/writes data directly from/to Volume Server
5. Metadata is stored in Master's DirectoryTree

---

## License

Open Source License To Be Determined (Planned: Apache 2.0 / MIT)

---

**PowerFS — Build the next-generation unified storage for HPC & AI super cluster.**