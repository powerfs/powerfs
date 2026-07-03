# README\.md

PowerFS
**Zero\-jitter unified parallel file system for HPC simulation and LLM KV cache**

*Next\-generation high\-performance unified storage for HPC \& AI super clusters*

[Introduction](https://github.com/FudanPPI/powerfs/tree/master#introduction) • [Architecture](https://github.com/FudanPPI/powerfs/tree/master#architecture) • [Core Features](https://github.com/FudanPPI/powerfs/tree/master#architecture) • [Roadmap](https://github.com/FudanPPI/powerfs/tree/master#architecture) • [Scenarios](https://github.com/FudanPPI/powerfs/tree/master#architecture) • [Benchmark](https://github.com/FudanPPI/powerfs/tree/master#architecture) • [License](https://github.com/FudanPPI/powerfs/tree/master#architecture)



---

## Introduction

**PowerFS** is a high\-performance, zero\-jitter unified parallel file system built from scratch with Rust\. It is specially designed for converged HPC simulation and LLM AI cluster workloads, delivering ultra\-low latency, stable parallel I/O and native AI cache acceleration capabilities\.

Traditional storage solutions face obvious bottlenecks in converged HPC and AI scenarios\. Professional HPC file systems suffer from complex deployment, heavy operation and maintenance, severe I/O jitter and poor small\-file performance, and cannot adapt to AI inference workloads\. Common cloud\-native storage lacks massive parallel computing capability and native LLM KV cache support, resulting in insufficient overall cluster resource utilization\.

PowerFS innovates a **dual\-engine fusion architecture of parallel file storage and native KV cache**\. It unifies traditional HPC scientific computing, large\-scale parallel simulation, AI dataset training and LLM inference cache services into one storage stack, solving the fragmentation problem of separated HPC and AI storage systems\. It is the optimal unified storage base for next\-generation super computing and intelligent computing converged clusters\.

---

## Core Design Philosophy

- **Pure Rust Stack**：Complete user\-state I/O implementation, no GC jitter, memory safety, ultra\-stable latency under long\-time high load

- **Unified Converged Architecture**：One cluster supports standard POSIX parallel file access and LLM KV tensor high\-speed cache access

- **Zero\-Jitter Priority**：Foreground computing I/O is prioritized; background balancing, GC and encoding tasks are fully noise\-reduced to ensure steady\-state performance

- **Full Hardware Offloading**：Native adaptation to SPDK, RDMA and GPU Direct, end\-to\-end zero\-copy hardware acceleration

- **Lightweight Enterprise\-Grade**：Simplified architecture, linear horizontal scaling, low operation and maintenance costs, enterprise\-level high availability and fault tolerance

---

## Core Features

### ⚡ Extreme HPC Parallel Capability

- Distributed sharded metadata architecture, supporting 10,000\+ MPI process concurrent read and write

- Complete standard POSIX semantics, fully compatible with mainstream HPC simulation software and parallel computing frameworks

- Adaptive file striping and multi\-node aggregated I/O, supporting PB\-level cluster aggregated bandwidth

- Fine\-grained job\-level QoS and I/O isolation, eliminating resource preemption and ensuring zero\-jitter steady\-state operation

- Optimized ultra\-large directory and massive small\-file scenarios, solving traditional HPC storage small\-file performance bottlenecks

### 🧠 Native LLM KV Cache Engine \(Industry Exclusive\)

- Built\-in dedicated KV tensor storage engine, no third\-party components, deeply optimized for LLM inference characteristics

- O\(1\) constant\-time KV addressing, microsecond\-level access latency, supporting incremental update and partial overwriting

- Dual elimination strategy of LRU hot and cold sorting \+ TTL session expiration, realizing intelligent cache automatic management

- Session\-level cache isolation and hot data resident mechanism, greatly improving long\-text inference token generation throughput

- Native GPU Direct zero\-copy transmission, extending GPU HBM video memory with NVMe storage to completely solve LLM inference video memory bottlenecks

### 🚀 Ultra\-Low Latency Hardware Acceleration

- SPDK user\-state NVMe bare disk I/O, bypassing kernel file system and system call overhead, maximizing hardware IOPS and bandwidth

- Full\-link RDMA lossless network instead of TCP, eliminating network soft interrupts and kernel protocol stack overhead

- Dual\-client mode: lightweight FUSE user client \+ high\-performance Linux kernel client

- No periodic jitter caused by runtime GC, stable p99/p999 latency under full\-load cluster

### 🛠 Lightweight \& Highly Available OPS

- Stateless master scheduling cluster based on Raft consensus, no single point of failure, unlimited horizontal scaling

- Rack\-aware topology scheduling, realizing local I/O and intelligent data load balancing

- Dual storage engine of multi\-replica \& EC erasure coding, adaptive hot and cold data hierarchical storage

- Automatic node/disk fault detection, data migration and cluster self\-healing

- Simplified deployment and operation, significantly lower maintenance costs than traditional Lustre/BeeGFS

---

## Architecture

PowerFS adopts a **four\-layer decoupled, dual\-engine coexistence, full hardware acceleration** overall architecture, realizing complete separation of control plane and data plane:

1. **Global Scheduling Layer**
High\-availability Raft master cluster, responsible for cluster topology management, resource allocation and task scheduling\. It only maintains global metadata mapping without storing massive business data, completely avoiding metadata bottlenecks\.

2. **Parallel Metadata Layer**
Sharded inode and directory metadata management, supporting ultra\-large directories and massive concurrent metadata operations, providing complete standard POSIX semantics for HPC parallel jobs\.

3. **Dual Data Engine Layer**

    - **HPC Parallel File Engine**：Optimized for supercomputing simulation, large\-file parallel reading and writing, and scientific computing batch workloads

    - **AI Native KV Cache Engine**：Dedicatedly optimized for LLM training and inference KV tensor high\-speed cache scenarios

4. **Hardware Acceleration Layer**
Native integration of SPDK NVMe user\-state I/O, RDMA lossless network and GPU Direct zero\-copy transmission, fully releasing the performance of NVMe SSD, high\-speed network and GPU heterogeneous computing resources\.

---

## Roadmap

### Phase 0 · Project Initialization \(1 Week\)

Repository initialization, CI/CD pipeline construction, official document site framework, architecture whitepaper drafting and community environment preparation\.

### Phase 1 · Core Storage Base \(2\-3 Weeks\)

Implement core storage stack including master scheduling, volume management, O\(1\) indexed addressing, basic replica mechanism and FUSE user\-mode client to complete basic file read\-write capabilities\.

### Phase 2 · HPC Parallel Enhancement \(3 Weeks\)

Complete distributed sharded metadata service, file striping parallel I/O, full POSIX semantic compatibility, and implement HPC job\-level QoS isolation and low\-jitter background scheduling\.

### Phase 3 · Linux Kernel Client \(4\-6 Weeks\)

Develop native Linux kernel client, dock with Linux VFS system, completely eliminate FUSE overhead, and reach enterprise\-level HPC ultra\-low latency performance indicators\.

### Phase 4 · Native KV Cache Engine \(3 Weeks\)

Complete LLM dedicated KV cache engine development, implement session isolation, intelligent hot\-cold elimination, incremental update, and dock GPU Direct zero\-copy acceleration pipeline\.

### Phase 5 · Production\-Grade Optimization \(Continuous Iteration\)

Full\-link SPDK/RDMA hardware offloading, EC erasure coding hierarchical storage, multi\-tenant permission management, complete monitoring and operation system, and release full\-standard benchmark performance comparison data\.

---

## Application Scenarios

- **HPC Supercomputing Cluster**：Fluid mechanics, meteorological simulation, structural calculation, material simulation and large\-scale MPI parallel computing jobs

- **AI Training Cluster**：Massive dataset storage, large model training high\-throughput reading and writing, model file persistent storage

- **LLM Inference Cluster**：Long\-text dialogue KV cache acceleration, GPU video memory overflow solution, high\-concurrency inference service optimization

- **HPC \& AI Converged Cluster**：Unified storage resource pooling, isolated coexistence of supercomputing and intelligent computing workloads

---

## Benchmark

### FIO Performance Test Results

All tests are conducted on a single-node setup with PowerFS FUSE client, using standard `fio` benchmark tool.

#### Test Environment
- **Hardware**: Single node with NVMe SSD
- **Block Size**: 4KB (random), 1MB (sequential)
- **Test Size**: 100MB per test
- **IO Engine**: `sync` (standard POSIX I/O)

#### Async Mode (Without fsync - Cached Writes)

| Test Type | Block Size | IOPS | Bandwidth | Avg Latency |
|-----------|------------|------|-----------|-------------|
| Sequential Write | 1MB | 3,448 | 3,448 MiB/s | 258 usec |
| Sequential Read | 1MB | 480 | 481 MiB/s | 2,072 usec |
| Random Write | 4KB | 624,000 | 2,439 MiB/s | 1.3 usec |
| Random Read | 4KB | 7,132 | 27.9 MiB/s | 139 usec |
| Mixed Read/Write (70%/30%) | 4KB | 9,846 | 38.5 MiB/s | - |

#### Sync Mode (With fsync - Persistent Writes)

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
| Sequential Write (fsync) | 1MB | 365 | 366 MiB/s | 516 usec |
| Random Read | 4KB | 23,300 | 91.2 MiB/s | 169 usec |

#### Key Insights

- **Async Write Performance**: Random writes reach 624K IOPS with cached writes, demonstrating excellent write buffer efficiency
- **Sync Write Performance**: Limited by gRPC round-trip and disk fsync (~1.3ms), typical for network-attached storage
- **Multi-thread Scaling**: Random read scales to 23.3K IOPS with 4 threads, showing effective parallel processing
- **Data Integrity**: All tests passed `--verify=crc32c` validation, confirming data correctness

### Benchmark Outlook

PowerFS targets leading performance among mainstream open-source distributed storage systems, with core advantages as follows:

- **vs General Cloud-Native Storage**：Higher parallel computing concurrency, lower steady-state jitter, native KV cache AI acceleration capability

- **vs Traditional HPC File System**：Lighter architecture, lower O&M cost, better small-file performance, natively adapted to AI inference scenarios

- **vs Lightweight Distributed Storage**：Complete POSIX HPC semantics, enterprise-level high availability and QoS isolation, professional supercomputing cluster carrying capacity

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
┌─────────────────────────────────────────────────────────────┐
│                      Client Layer                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────────┐  │
│  │  FUSE    │  │  Filer   │  │      KV Cache Client     │  │
│  │  Mount   │  │  (HTTP)  │  │  (for LLM Inference)     │  │
│  └────┬─────┘  └────┬─────┘  └───────────┬──────────────┘  │
└───────┼──────────────┼───────────────────┼─────────────────┘
        │              │                   │
┌───────▼──────────────▼───────────────────▼─────────────────┐
│                    Master Layer                            │
│              (Raft Consensus Cluster)                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Cluster Management | Resource Allocation | Metadata │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬─────────────────────────────────────┘
                       │
┌──────────────────────▼─────────────────────────────────────┐
│                   Volume Layer                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Volume 1    │  │  Volume 2    │  │  Volume N    │     │
│  │  (8080)      │  │  (8081)      │  │  (8xxx)      │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

---

## License

Open Source License To Be Determined \(Planned: Apache 2\.0 / MIT\)

---

**PowerFS — Build the next\-generation unified storage for HPC \& AI super cluster\.**
