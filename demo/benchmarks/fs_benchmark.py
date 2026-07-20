#!/usr/bin/env python3
import time
import os
import json
import random
from collections import defaultdict

class BenchmarkResult:
    def __init__(self, operation, count, size_bytes, duration_ms):
        self.operation = operation
        self.count = count
        self.size_bytes = size_bytes
        self.duration_ms = duration_ms
        self.ops_per_sec = count / (duration_ms / 1000) if duration_ms > 0 else 0
        self.bandwidth_mbps = (size_bytes * 8 / 1000000) / (duration_ms / 1000) if duration_ms > 0 else 0
        self.avg_latency_ms = duration_ms / count if count > 0 else 0

    def to_dict(self):
        return {
            "operation": self.operation,
            "count": self.count,
            "size_bytes": self.size_bytes,
            "duration_ms": round(self.duration_ms, 2),
            "ops_per_sec": round(self.ops_per_sec, 2),
            "bandwidth_mbps": round(self.bandwidth_mbps, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 4)
        }

def generate_random_data(size):
    return os.urandom(size)

def run_fs_benchmark(base_path, test_sizes=[64*1024, 256*1024, 1024*1024]):
    results = []
    
    size_labels = [f"{s//1024}KB" for s in test_sizes]
    print(f"🔄 FS Benchmark (sizes: {', '.join(size_labels)})")
    print("=" * 80)
    
    os.makedirs(base_path, exist_ok=True)
    
    for size in test_sizes:
        size_kb = size // 1024
        
        start = time.time()
        data = generate_random_data(size)
        for i in range(10):
            file_path = os.path.join(base_path, f"write_{size_kb}k_{i:03d}.dat")
            with open(file_path, "wb") as f:
                f.write(data)
        duration_ms = (time.time() - start) * 1000
        result = BenchmarkResult(f"WRITE_{size_kb}KB", 10, size * 10, duration_ms)
        results.append(result)
        print(f"✅ WRITE {size_kb}KB: {result.bandwidth_mbps:.2f} MB/s | {result.avg_latency_ms:.4f} ms avg")
        
        start = time.time()
        for i in range(10):
            file_path = os.path.join(base_path, f"write_{size_kb}k_{i:03d}.dat")
            with open(file_path, "rb") as f:
                content = f.read()
        duration_ms = (time.time() - start) * 1000
        result = BenchmarkResult(f"READ_{size_kb}KB", 10, size * 10, duration_ms)
        results.append(result)
        print(f"✅ READ {size_kb}KB: {result.bandwidth_mbps:.2f} MB/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for i in range(1000):
        file_path = os.path.join(base_path, f"small_{i:06d}.txt")
        with open(file_path, "w") as f:
            f.write("test")
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("CREATE_SMALL", 1000, 4000, duration_ms)
    results.append(result)
    print(f"✅ CREATE_SMALL: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for i in range(1000):
        file_path = os.path.join(base_path, f"small_{i:06d}.txt")
        os.remove(file_path)
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("DELETE_SMALL", 1000, 0, duration_ms)
    results.append(result)
    print(f"✅ DELETE_SMALL: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    for size in test_sizes:
        size_kb = size // 1024
        for i in range(10):
            try:
                os.remove(os.path.join(base_path, f"write_{size_kb}k_{i:03d}.dat"))
            except:
                pass
    
    return results

def main():
    print("🚀 PowerFS Filesystem Benchmark")
    print("=" * 80)
    print("Config:")
    print("  - Rounds: 2")
    print("  - Test sizes: 64KB, 256KB, 1MB")
    print("  - Small files: 1000 x 4 bytes")
    print("=" * 80)
    
    fuse_path = "/mnt/fuse1/fs_bench"
    fallback_path = "/tmp/fs_bench"
    
    base_path = fuse_path if os.path.exists(fuse_path) else fallback_path
    print(f"\nUsing path: {base_path}")
    
    all_results = []
    
    for round_num in range(1, 3):
        print(f"\n🔹 Round {round_num}")
        results = run_fs_benchmark(base_path)
        all_results.extend(results)
    
    print("\n📊 Summary Report")
    print("=" * 80)
    
    agg_results = defaultdict(list)
    for r in all_results:
        agg_results[r.operation].append(r)
    
    print(f"{'OPERATION':<15} | {'ROUNDS':<6} | {'BW(MB/s)':<12} | {'AVG LATENCY(ms)':<15}")
    print("-" * 80)
    for op, results in agg_results.items():
        avg_bw = sum(r.bandwidth_mbps for r in results) / len(results)
        avg_latency = sum(r.avg_latency_ms for r in results) / len(results)
        print(f"{op:<15} | {len(results):<6} | {avg_bw:<12.2f} | {avg_latency:<15.4f}")
    
    os.makedirs("/results", exist_ok=True)
    report = {
        "benchmark": "fs",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "config": {"rounds": 2, "test_sizes": [64*1024, 256*1024, 1024*1024], "test_path": base_path},
        "operations": [r.to_dict() for r in all_results],
        "summary": {
            op: {
                "avg_bandwidth_mbps": round(sum(r.bandwidth_mbps for r in results) / len(results), 2),
                "avg_latency_ms": round(sum(r.avg_latency_ms for r in results) / len(results), 4)
            } for op, results in agg_results.items()
        }
    }
    
    with open("/results/fs_benchmark.json", "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n📈 Results saved to /results/fs_benchmark.json")

if __name__ == "__main__":
    main()