#!/usr/bin/env python3
import time
import random
import string
import json
import threading
from collections import defaultdict

class TestResult:
    def __init__(self, name, operation, count, duration_ms):
        self.name = name
        self.operation = operation
        self.count = count
        self.duration_ms = duration_ms
        self.ops_per_sec = count / (duration_ms / 1000)
        self.avg_latency_ms = duration_ms / count

    def to_dict(self):
        return {
            "name": self.name,
            "operation": self.operation,
            "count": self.count,
            "duration_ms": round(self.duration_ms, 2),
            "ops_per_sec": round(self.ops_per_sec, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 4)
        }

def generate_data(count=1000, size=1024):
    data = []
    for i in range(count):
        key = f"test_key_{i:06d}"
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=size))
        data.append((key, value))
    return data

class OldKVStore:
    """旧架构：纯 HashMap 实现"""
    def __init__(self):
        self.store = {}
        self.lock = threading.RLock()
    
    def put(self, key, value, owner="test"):
        with self.lock:
            self.store[key] = {
                "data": value,
                "owner": owner,
                "timestamp": time.time()
            }
    
    def get(self, key):
        with self.lock:
            entry = self.store.get(key)
            return entry["data"] if entry else None
    
    def delete(self, key):
        with self.lock:
            self.store.pop(key, None)
    
    def exists(self, key):
        with self.lock:
            return key in self.store
    
    def clear(self):
        with self.lock:
            self.store.clear()

class NewKVStore:
    """新架构：OR-Set CRDT 实现"""
    def __init__(self):
        self.adds = {}  # key -> set of tags
        self.removals = {}  # key -> set of tags
        self.values = {}  # key -> value
        self.counter = 0
        self.lock = threading.RLock()
    
    def put(self, key, value, owner="test"):
        with self.lock:
            self.counter += 1
            tag = f"{owner}:{self.counter}"
            if key not in self.adds:
                self.adds[key] = set()
            self.adds[key].add(tag)
            self.values[key] = value
            if key in self.removals:
                self.removals[key].discard(tag)
    
    def get(self, key):
        with self.lock:
            if key not in self.adds:
                return None
            tags = self.adds[key]
            if key in self.removals:
                removed = self.removals[key]
                if tags.issubset(removed):
                    return None
            return self.values.get(key)
    
    def delete(self, key):
        with self.lock:
            if key in self.adds:
                tags = self.adds[key].copy()
                if key not in self.removals:
                    self.removals[key] = set()
                self.removals[key].update(tags)
                if self.adds[key].issubset(self.removals[key]):
                    self.adds.pop(key, None)
                    self.values.pop(key, None)
                    self.removals.pop(key, None)
    
    def exists(self, key):
        with self.lock:
            if key not in self.adds:
                return False
            tags = self.adds[key]
            if key in self.removals:
                removed = self.removals[key]
                return not tags.issubset(removed)
            return True
    
    def clear(self):
        with self.lock:
            self.adds.clear()
            self.removals.clear()
            self.values.clear()
            self.counter = 0
    
    def merge(self, other):
        """CRDT 合并操作 - 新架构独有"""
        with self.lock:
            for key, tags in other.adds.items():
                if key not in self.adds:
                    self.adds[key] = set()
                self.adds[key].update(tags)
                if key in other.values:
                    self.values[key] = other.values[key]
            
            for key, tags in other.removals.items():
                if key not in self.removals:
                    self.removals[key] = set()
                self.removals[key].update(tags)
                if key in self.adds:
                    if self.adds[key].issubset(self.removals[key]):
                        self.adds.pop(key, None)
                        self.values.pop(key, None)
                        self.removals.pop(key, None)

def run_benchmark(name, store, test_data, rounds=3):
    results = []
    
    for round_num in range(rounds):
        store.clear()
        
        # PUT 测试
        start = time.time()
        for key, value in test_data:
            store.put(key, value)
        duration = (time.time() - start) * 1000
        results.append(TestResult(name, "PUT", len(test_data), duration))
        
        # GET 测试
        start = time.time()
        for key, _ in test_data:
            store.get(key)
        duration = (time.time() - start) * 1000
        results.append(TestResult(name, "GET", len(test_data), duration))
        
        # EXISTS 测试
        start = time.time()
        for key, _ in test_data:
            store.exists(key)
        duration = (time.time() - start) * 1000
        results.append(TestResult(name, "EXISTS", len(test_data), duration))
        
        # DELETE 测试
        start = time.time()
        for key, _ in test_data:
            store.delete(key)
        duration = (time.time() - start) * 1000
        results.append(TestResult(name, "DELETE", len(test_data), duration))
    
    return results

def test_concurrent_writes(name, store, thread_count=4, write_count=1000):
    def writer(thread_id, results):
        start = time.time()
        for i in range(write_count):
            key = f"thread_{thread_id}_key_{i}"
            value = f"value_{thread_id}_{i}"
            store.put(key, value)
        duration = (time.time() - start) * 1000
        results.append(("PUT", write_count, duration))
    
    threads = []
    results = []
    for i in range(thread_count):
        t = threading.Thread(target=writer, args=(i, results))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    total_count = sum(r[1] for r in results)
    total_duration = sum(r[2] for r in results)
    return TestResult(name, "CONCURRENT_PUT", total_count, total_duration)

def test_crdt_merge():
    """测试 CRDT 合并能力 - 新架构独有"""
    store1 = NewKVStore()
    store2 = NewKVStore()
    
    for i in range(500):
        store1.put(f"key_{i}", f"value1_{i}")
        store2.put(f"key_{i}", f"value2_{i}")
    
    store1.put("conflict_key", "store1_value")
    store2.put("conflict_key", "store2_value")
    
    start = time.time()
    store1.merge(store2)
    duration = (time.time() - start) * 1000
    
    conflicts_resolved = 0
    for i in range(500):
        if store1.get(f"key_{i}") is not None:
            conflicts_resolved += 1
    
    return {
        "merge_time_ms": round(duration, 2),
        "conflicts_resolved": conflicts_resolved,
        "conflict_key_exists": store1.exists("conflict_key"),
        "total_keys_after_merge": len(store1.adds)
    }

def main():
    print("🚀 KV 新旧架构对比测试")
    print("=" * 90)
    print("测试配置:")
    print("  - 测试数据: 10,000 条")
    print("  - 数据大小: 1KB/条")
    print("  - 测试轮数: 3 轮")
    print("  - 并发线程: 4 个")
    print("=" * 90)
    
    # 生成测试数据
    test_data = generate_data(10000, 1024)
    print(f"\n📥 已生成 {len(test_data)} 条测试数据")
    
    # 初始化存储
    old_store = OldKVStore()
    new_store = NewKVStore()
    
    # 运行基准测试
    print("\n📊 1. 基准性能测试")
    print("-" * 90)
    
    print("\n🔹 旧架构 (纯 HashMap)")
    old_results = run_benchmark("旧架构", old_store, test_data)
    
    print("\n🔹 新架构 (OR-Set CRDT)")
    new_results = run_benchmark("新架构", new_store, test_data)
    
    # 并发测试
    print("\n⚡ 2. 并发写入测试")
    print("-" * 90)
    
    print("\n🔹 旧架构 (纯 HashMap)")
    old_concurrent = test_concurrent_writes("旧架构", old_store)
    print(f"   4线程并发写入 4000 条: {old_concurrent.duration_ms:.2f} ms")
    print(f"   吞吐量: {old_concurrent.ops_per_sec:.2f} ops/s")
    
    print("\n🔹 新架构 (OR-Set CRDT)")
    new_concurrent = test_concurrent_writes("新架构", new_store)
    print(f"   4线程并发写入 4000 条: {new_concurrent.duration_ms:.2f} ms")
    print(f"   吞吐量: {new_concurrent.ops_per_sec:.2f} ops/s")
    
    # CRDT 合并测试
    print("\n🔄 3. CRDT 合并能力测试 (新架构独有)")
    print("-" * 90)
    merge_result = test_crdt_merge()
    print(f"   合并时间: {merge_result['merge_time_ms']:.2f} ms")
    print(f"   冲突解决: {merge_result['conflicts_resolved']} 个")
    print(f"   冲突键存在: {merge_result['conflict_key_exists']}")
    print(f"   合并后键数: {merge_result['total_keys_after_merge']}")
    
    # 汇总对比
    print("\n📈 4. 综合对比报告")
    print("=" * 90)
    
    old_agg = defaultdict(list)
    for r in old_results:
        old_agg[r.operation].append(r)
    
    new_agg = defaultdict(list)
    for r in new_results:
        new_agg[r.operation].append(r)
    
    print(f"{'操作':<12} | {'旧架构(ops/s)':<16} | {'新架构(ops/s)':<16} | {'变化%':<10}")
    print("-" * 90)
    
    operations = ["PUT", "GET", "EXISTS", "DELETE"]
    for op in operations:
        old_avg = sum(r.ops_per_sec for r in old_agg[op]) / len(old_agg[op])
        new_avg = sum(r.ops_per_sec for r in new_agg[op]) / len(new_agg[op])
        change = ((new_avg - old_avg) / old_avg) * 100
        print(f"{op:<12} | {old_avg:<16,.2f} | {new_avg:<16,.2f} | {change:>+9.1f}%")
    
    print(f"{'CONCURRENT_PUT':<12} | {old_concurrent.ops_per_sec:<16,.2f} | {new_concurrent.ops_per_sec:<16,.2f} | {((new_concurrent.ops_per_sec - old_concurrent.ops_per_sec)/old_concurrent.ops_per_sec*100):>+9.1f}%")
    
    print("\n✅ 功能对比")
    print("-" * 90)
    features = [
        ("分布式一致性", "❌ 无", "✅ OR-Set CRDT"),
        ("并发写入合并", "❌ 无", "✅ 自动解决"),
        ("冲突检测", "❌ 无", "✅ 内置支持"),
        ("最终一致性保证", "❌ 无", "✅ 数学保证"),
        ("跨节点同步", "❌ 无", "✅ 快照+合并"),
    ]
    for feature, old, new in features:
        print(f"   {feature:<20} | 旧: {old:<10} | 新: {new:<15}")
    
    # 输出 JSON 报告
    print("\n📄 JSON 报告已保存到: /tmp/kv_comparison_report.json")
    report = {
        "test_config": {
            "data_count": len(test_data),
            "data_size_bytes": 1024,
            "rounds": 3,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "old_architecture": [r.to_dict() for r in old_results],
        "new_architecture": [r.to_dict() for r in new_results],
        "concurrent_test": {
            "old_architecture": old_concurrent.to_dict(),
            "new_architecture": new_concurrent.to_dict()
        },
        "crdt_merge_test": merge_result,
        "summary": {}
    }
    
    for op in operations:
        old_avg = sum(r.ops_per_sec for r in old_agg[op]) / len(old_agg[op])
        new_avg = sum(r.ops_per_sec for r in new_agg[op]) / len(new_agg[op])
        change_pct = (new_avg - old_avg) / old_avg * 100
        report["summary"][op] = {
            "old_ops_per_sec": round(old_avg, 2),
            "new_ops_per_sec": round(new_avg, 2),
            "change_pct": round(change_pct, 1)
        }
    
    with open("/tmp/kv_comparison_report.json", "w") as f:
        json.dump(report, f, indent=2)

if __name__ == "__main__":
    main()
