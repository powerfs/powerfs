#!/usr/bin/env python3
import time
import random
import string
import json
import os
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

def generate_random_string(length=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

class MockKVClient:
    def __init__(self):
        self.store = {}
    
    def put(self, namespace, key, value, owner):
        full_key = f"{namespace}:{key}"
        self.store[full_key] = {
            "data": value,
            "owner_id": owner,
            "created_at": int(time.time()),
            "updated_at": int(time.time())
        }
    
    def get(self, namespace, key):
        full_key = f"{namespace}:{key}"
        entry = self.store.get(full_key)
        return entry["data"] if entry else None
    
    def exists(self, namespace, key):
        full_key = f"{namespace}:{key}"
        return full_key in self.store
    
    def list(self, namespace, prefix):
        prefix_key = f"{namespace}:{prefix}"
        return [k.split(f"{namespace}:")[1] for k in self.store.keys() if k.startswith(prefix_key)]
    
    def delete(self, namespace, key):
        full_key = f"{namespace}:{key}"
        self.store.pop(full_key, None)

def run_kv_benchmark(kv_client, iterations=1000):
    results = []
    test_keys = []
    test_values = []
    
    print(f"🔄 KV Benchmark (iterations: {iterations})")
    print("=" * 80)
    
    start = time.time()
    for i in range(iterations):
        key = f"bench_key_{i:06d}"
        value = generate_random_string(1024)
        test_keys.append(key)
        test_values.append(value)
        kv_client.put("test_ns", key, value, "test_user")
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("PUT", iterations, duration_ms)
    results.append(result)
    print(f"✅ PUT: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for i in range(iterations):
        key = test_keys[i]
        val = kv_client.get("test_ns", key)
        assert val is not None, f"GET failed for {key}"
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("GET", iterations, duration_ms)
    results.append(result)
    print(f"✅ GET: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    for i in range(iterations):
        key = test_keys[i]
        exists = kv_client.exists("test_ns", key)
        assert exists, f"EXISTS failed for {key}"
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("EXISTS", iterations, duration_ms)
    results.append(result)
    print(f"✅ EXISTS: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    start = time.time()
    keys = kv_client.list("test_ns", "bench_key_")
    assert len(keys) >= iterations, f"LIST returned only {len(keys)} keys"
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("LIST", 1, duration_ms)
    results.append(result)
    print(f"✅ LIST: {len(keys)} keys | {result.duration_ms:.2f} ms")
    
    start = time.time()
    for i in range(iterations):
        key = test_keys[i]
        kv_client.delete("test_ns", key)
    duration_ms = (time.time() - start) * 1000
    result = BenchmarkResult("DELETE", iterations, duration_ms)
    results.append(result)
    print(f"✅ DELETE: {result.ops_per_sec:.2f} ops/s | {result.avg_latency_ms:.4f} ms avg")
    
    return results

def main():
    print("🚀 PowerFS KV Storage Benchmark")
    print("=" * 80)
    print("Config:")
    print("  - Rounds: 3")
    print("  - Iterations: 10,000 per round")
    print("  - Data size: 1KB per entry")
    print("=" * 80)
    
    kv_client = MockKVClient()
    all_results = []
    
    for round_num in range(1, 4):
        print(f"\n🔹 Round {round_num}")
        results = run_kv_benchmark(kv_client, 10000)
        all_results.extend(results)
    
    print("\n📊 Summary Report")
    print("=" * 80)
    
    agg_results = defaultdict(list)
    for r in all_results:
        agg_results[r.operation].append(r)
    
    print(f"{'OPERATION':<10} | {'ROUNDS':<6} | {'AVG OPS/S':<12} | {'AVG LATENCY(ms)':<15}")
    print("-" * 80)
    for op, results in agg_results.items():
        avg_ops = sum(r.ops_per_sec for r in results) / len(results)
        avg_latency = sum(r.avg_latency_ms for r in results) / len(results)
        print(f"{op:<10} | {len(results):<6} | {avg_ops:<12.2f} | {avg_latency:<15.4f}")
    
    os.makedirs("/results", exist_ok=True)
    report = {
        "benchmark": "kv",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "config": {"rounds": 3, "iterations_per_round": 10000, "data_size_bytes": 1024},
        "operations": [r.to_dict() for r in all_results],
        "summary": {
            op: {
                "avg_ops_per_sec": round(sum(r.ops_per_sec for r in results) / len(results), 2),
                "avg_latency_ms": round(sum(r.avg_latency_ms for r in results) / len(results), 4)
            } for op, results in agg_results.items()
        }
    }
    
    with open("/results/kv_benchmark.json", "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n📈 Results saved to /results/kv_benchmark.json")

if __name__ == "__main__":
    main()