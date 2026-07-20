#!/bin/bash

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DEMO_DIR=$(dirname "$SCRIPT_DIR")

echo "============================================"
echo "      Running PowerFS Benchmarks             "
echo "============================================"

mkdir -p "$DEMO_DIR/results"
docker exec demo-benchmark mkdir -p /results

echo ""
echo "1. Running KV Benchmark..."
docker exec demo-benchmark python /benchmarks/kv_benchmark.py

echo ""
echo "2. Running Metadata Benchmark..."
docker exec demo-benchmark python /benchmarks/metadata_benchmark.py

echo ""
echo "3. Running FUSE Benchmark..."
docker exec demo-benchmark python /benchmarks/fs_benchmark.py

echo ""
echo "4. Generating Report..."
docker exec demo-benchmark python /benchmarks/report_generator.py

echo ""
echo "============================================"
echo "      Benchmarks Complete!                   "
echo "============================================"
echo ""
echo "Results saved to:"
echo "  - $DEMO_DIR/results/"
echo "  - $DEMO_DIR/results/report.html (HTML report)"
echo ""