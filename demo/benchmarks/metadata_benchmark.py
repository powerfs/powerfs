#!/usr/bin/env python3
import time
import os
import json
import random
import string
from collections import defaultdict

class BenchmarkResult:
    def __init__(self, operation, count, duration_ms):
        self.operation = operation
        self.count = count
        self.duration_ms = duration_ms
        self.ops_per_sec = count / (duration_ms / 1000) if duration_ms > 0 else 0
        self.avg_latency_ms = duration_ms / count if count > 0 else 0

    def to_dict(self):
        return {
            "operation": self.operation,
            "count": self.count,
            "duration_ms": round(self.duration_ms, 2),
            "ops_per_sec": round(self.ops_per_sec, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 4)
        }

def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def run_metadata_benchmark(base_path, iterations=100):
    results = []
    paths = []
    
    print(f"🔄 Metadata Benchmark (iterations: {iterations})")
    print("=" * 80)
    
    os.makedirs(base_path, exist_ok=True)
    
    start = time.time()
    for i in range(iterations):
        dir_path = os.path.join(base_path, f"dir_{i:06d}_{generate_random_name()}")
        os.makedirs(dir_path, exist_ok=True)
        paths.append(dir_path)
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("CREATE_DIR", iterations, duration_ms)
    results.append(result)
    print(f"✅ CREATE_DIR: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for i in range(iterations):
        file_path = os.path.join(base_path, f"file_{i:06d}_{generate_random_name()}.txt")
        with open(file_path, "w") as f:
            f.write(generate_random_string(1024))
        paths.append(file_path)
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("CREATE_FILE", iterations, duration_ms)
    results.append(result)
    print(f"✅ CREATE_FILE: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for path in paths:
        if os.path.isfile(path):
            with open(path, "r") as f:
                content = f.read()
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("READ_FILE", iterations, duration_ms)
    results.append(result)
    print(f"✅ READ_FILE: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for i, path in enumerate(paths):
        if os.path.isfile(path):
            new_name = f"renamed_{i:06d}.txt"
            new_path = os.path.join(os.path.dirname(path), new_name)
            os.rename(path, new_path)
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("RENAME", iterations, duration_ms)
    results.append(result)
    print(f"✅ RENAME: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for i in range(iterations):
        dir_path = os.path.join(base_path, f"list_test_{i:03d}")
        os.makedirs(dir_path, exist_ok=True)
        for j in range(10):
            file_path = os.path.join(dir_path, f"item_{j:03d}.txt")
            with open(file_path, "w") as f:
                f.write("test")
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("LIST_PREP", iterations * 10, duration_ms)
    results.append(result)
    
    start = time.time()
    for i in range(iterations):
        dir_path = os.path.join(base_path, f"list_test_{i:03d}")
        entries = os.listdir(dir_path)
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("LIST_DIR", iterations, duration_ms)
    results.append(result)
    print(f"✅ LIST_DIR: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for path in paths:
        try:
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                os.rmdir(path)
        except:
            pass
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("DELETE", len(paths), duration_ms)
    results.append(result)
    print(f"✅ DELETE: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    for i in range(iterations):
        dir_path = os.path.join(base_path, f"list_test_{i:03d}")
        try:
            for f in os.listdir(dir_path):
                os.remove(os.path.join(dir_path, f))
            os.rmdir(dir_path)
        except:
            pass
    
    return results

def generate_random_string(length=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def main():
    print("🚀 PowerFS Metadata Benchmark")
    print("=" * 80)
    print("Config:")
    print("  - Rounds: 2")
    print("  - Iterations: 500 per round")
    print("  - Test path: /mnt/fuse1/benchmark")
    print("=" * 80)
    
    fuse_path = "/mnt/fuse1/benchmark"
    fallback_path = "/tmp/benchmark"
    
    base_path = fuse_path if os.path.exists(fuse_path) else fallback_path
    print(f"\nUsing path: {base_path}")
    
    all_results = []
    
    for round_num in range(1, 3):
        print(f"\n🔹 Round {round_num}")
        results = run_metadata_benchmark(base_path, 500)
        all_results.extend(results)
    
    print("\n📊 Summary Report")
    print("=" * 80)
    
    agg_results = defaultdict(list)
    for r in all_results:
        agg_results[r.operation].append(r)
    
    print(f"{'OPERATION':<12} | {'ROUNDS':<6} | {'AVG OPS/S':<12} | {'AVG LATENCY(ms)':<15}")
    print("-" * 80)
    for op, results in agg_results.items():
        avg_ops = sum(r.ops_per_sec for r in results) / len(results)
        avg_latency = sum(r.avg_latency_ms for r in results) / len(results)
        print(f"{op:<12} | {len(results):<6} | {avg_ops:<12.2f} | {avg_latency:<15.4f}")
    
    os.makedirs("/results", exist_ok=True)
    report = {
        "benchmark": "metadata",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "config": {"rounds": 2, "iterations_per_round": 500, "test_path": base_path},
        "operations": [r.to_dict() for r in all_results],
        "summary": {
            op: {
                "avg_ops_per_sec": round(sum(r.ops_per_sec for r in results) / len(results), 2),
                "avg_latency_ms": round(sum(r.avg_latency_ms for r in results) / len(results), 4)
            } for op, results in agg_results.items()
        }
    }
    
    with open("/results/metadata_benchmark.json", "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n📈 Results saved to /results/metadata_benchmark.json")

if __name__ == "__main__":
    main()