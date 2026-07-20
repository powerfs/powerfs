# PowerFS Demo Environment

演示环境用于展示 PowerFS 的核心功能和性能测试。

## 快速开始

### 1. 启动演示环境

```bash
cd demo
./scripts/start-demo.sh
```

### 2. 运行性能测试

```bash
./scripts/run-benchmarks.sh
```

### 3. 停止演示环境

```bash
./scripts/stop-demo.sh
```

## 服务访问

| 服务 | 地址 |
|------|------|
| 监控面板 | http://localhost:8084 |
| S3 API | http://localhost:9000 |
| Master 节点 | localhost:9333, 9334, 9335 |
| Volume 节点 | localhost:8080, 8081, 8082 |
| Redis | localhost:6379 |
| FUSE 挂载点 1 | /tmp/powerfs-demo/fuse1 |
| FUSE 挂载点 2 | /tmp/powerfs-demo/fuse2 |

## 性能测试

### KV 存储测试

测试 PUT/GET/EXISTS/LIST/DELETE 操作的吞吐量和延迟。

### 元数据测试

测试目录创建、文件创建、读取、重命名、列表查询等元数据操作性能。

### 文件系统测试

测试不同大小文件的读写带宽和小文件创建/删除性能。

## 测试结果

测试结果保存在 `demo/results/` 目录：

- `kv_benchmark.json` - KV 测试结果
- `metadata_benchmark.json` - 元数据测试结果
- `fs_benchmark.json` - 文件系统测试结果
- `report.html` - HTML 可视化报告

## 目录结构

```
demo/
├── docker-compose.demo.yml    # Docker Compose 配置
├── scripts/                   # 操作脚本
│   ├── start-demo.sh          # 启动演示环境
│   ├── stop-demo.sh           # 停止演示环境
│   └── run-benchmarks.sh      # 运行性能测试
├── benchmarks/                # 性能测试脚本
│   ├── kv_benchmark.py        # KV 存储测试
│   ├── metadata_benchmark.py  # 元数据测试
│   ├── fs_benchmark.py        # 文件系统测试
│   └── report_generator.py    # 报告生成器
└── results/                   # 测试结果（运行后生成）
```