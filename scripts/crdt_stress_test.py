#!/usr/bin/env python3
"""
PowerFS CRDT 压力测试脚本
测试大量并发元数据操作的性能和正确性
"""

import os
import sys
import time
import json
import random
import threading
import statistics
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置
MOUNT_POINTS = [
    "/tmp/powerfs/crdt-fuse1",
    "/tmp/powerfs/crdt-fuse2",
]

TEST_CONFIG = {
    "num_files": 1000,          # 测试文件数量
    "num_threads": 10,          # 并发线程数
    "test_duration": 30,        # 测试持续时间（秒）
    "operations_per_thread": 100,  # 每个线程的操作数
}


class Metrics:
    """收集测试指标"""

    def __init__(self):
        self.lock = threading.Lock()
        self.operations = []
        self.errors = []
        self.start_time = None
        self.end_time = None

    def record_operation(self, op_type: str, latency_ms: float, success: bool):
        with self.lock:
            self.operations.append({
                "type": op_type,
                "latency_ms": latency_ms,
                "success": success,
                "timestamp": time.time()
            })

    def record_error(self, error: str):
        with self.lock:
            self.errors.append({
                "error": error,
                "timestamp": time.time()
            })

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.end_time = time.time()

    def get_duration(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0

    def get_summary(self) -> Dict[str, Any]:
        ops = [op for op in self.operations if op["success"]]
        failed = [op for op in self.operations if not op["success"]]

        durations = [op["latency_ms"] for op in ops]

        summary = {
            "total_operations": len(self.operations),
            "successful": len(ops),
            "failed": len(failed),
            "success_rate": len(ops) / max(len(self.operations), 1) * 100,
            "duration_sec": self.get_duration(),
            "throughput_ops_per_sec": len(ops) / max(self.get_duration(), 0.001),
            "latency": {},
            "operations_by_type": {}
        }

        if durations:
            sorted_durations = sorted(durations)
            summary["latency"] = {
                "min_ms": min(durations),
                "max_ms": max(durations),
                "avg_ms": statistics.mean(durations),
                "p50_ms": sorted_durations[len(sorted_durations) // 2],
                "p95_ms": sorted_durations[int(len(sorted_durations) * 0.95)],
                "p99_ms": sorted_durations[int(len(sorted_durations) * 0.99)],
            }

        # 按操作类型分组
        for op_type in set(op["type"] for op in self.operations):
            type_ops = [op for op in self.operations if op["type"] == op_type and op["success"]]
            type_durations = [op["latency_ms"] for op in type_ops]
            summary["operations_by_type"][op_type] = {
                "count": len(type_ops),
                "avg_ms": statistics.mean(type_durations) if type_durations else 0,
                "p99_ms": sorted(type_durations)[int(len(type_durations) * 0.99)] if type_durations else 0,
            }

        return summary


class PowerFSStressTest:
    def __init__(self):
        self.metrics = Metrics()
        self.test_dirs = []

    def setup(self):
        """创建测试目录"""
        print("设置测试环境...")
        for i, mount in enumerate(MOUNT_POINTS):
            test_dir = f"{mount}/stress-test-{i}"
            os.makedirs(test_dir, exist_ok=True)
            self.test_dirs.append(test_dir)
            print(f"  创建测试目录: {test_dir}")

    def cleanup(self):
        """清理测试数据"""
        print("清理测试数据...")
        for test_dir in self.test_dirs:
            try:
                for root, dirs, files in os.walk(test_dir, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                os.rmdir(test_dir)
                print(f"  清理: {test_dir}")
            except Exception as e:
                print(f"  清理失败 {test_dir}: {e}")

    # ========================================================================
    # 操作类型
    # ========================================================================

    def create_file(self, dir_path: str, file_name: str, content: str) -> bool:
        """创建文件"""
        start = time.time()
        try:
            file_path = f"{dir_path}/{file_name}"
            with open(file_path, 'w') as f:
                f.write(content)
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("create", latency, True)
            return True
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("create", latency, False)
            self.metrics.record_error(str(e))
            return False

    def read_file(self, dir_path: str, file_name: str) -> bool:
        """读取文件"""
        start = time.time()
        try:
            file_path = f"{dir_path}/{file_name}"
            with open(file_path, 'r') as f:
                _ = f.read()
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("read", latency, True)
            return True
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("read", latency, False)
            self.metrics.record_error(str(e))
            return False

    def write_file(self, dir_path: str, file_name: str, content: str) -> bool:
        """写入文件（覆盖）"""
        start = time.time()
        try:
            file_path = f"{dir_path}/{file_name}"
            with open(file_path, 'w') as f:
                f.write(content)
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("write", latency, True)
            return True
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("write", latency, False)
            self.metrics.record_error(str(e))
            return False

    def delete_file(self, dir_path: str, file_name: str) -> bool:
        """删除文件"""
        start = time.time()
        try:
            file_path = f"{dir_path}/{file_name}"
            os.remove(file_path)
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("delete", latency, True)
            return True
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("delete", latency, False)
            self.metrics.record_error(str(e))
            return False

    def list_dir(self, dir_path: str) -> bool:
        """列出目录"""
        start = time.time()
        try:
            _ = os.listdir(dir_path)
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("list", latency, True)
            return True
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.metrics.record_operation("list", latency, False)
            self.metrics.record_error(str(e))
            return False

    # ========================================================================
    # 压力测试场景
    # ========================================================================

    def test_concurrent_creates(self):
        """并发创建文件测试"""
        print("\n" + "=" * 60)
        print("并发创建文件测试")
        print("=" * 60)

        self.metrics.start()

        num_files = TEST_CONFIG["num_files"]
        num_threads = TEST_CONFIG["num_threads"]
        test_dir = self.test_dirs[0]

        print(f"  创建 {num_files} 个文件，使用 {num_threads} 个线程...")

        def create_batch(thread_id: int):
            for i in range(thread_id, num_files, num_threads):
                file_name = f"stress-file-{i}.txt"
                content = f"Content from thread {thread_id}, file {i}, random: {random.randint(0, 1000)}"
                self.create_file(test_dir, file_name, content)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(create_batch, t) for t in range(num_threads)]
            for f in as_completed(futures):
                f.result()

        self.metrics.stop()
        self._print_summary()

    def test_concurrent_reads(self):
        """并发读取文件测试"""
        print("\n" + "=" * 60)
        print("并发读取文件测试")
        print("=" * 60)

        self.metrics.start()

        num_files = TEST_CONFIG["num_files"]
        num_threads = TEST_CONFIG["num_threads"]
        test_dir = self.test_dirs[0]

        print(f"  读取 {num_files} 个文件，使用 {num_threads} 个线程...")

        def read_batch(thread_id: int):
            for i in range(thread_id, num_files, num_threads):
                file_name = f"stress-file-{i}.txt"
                self.read_file(test_dir, file_name)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(read_batch, t) for t in range(num_threads)]
            for f in as_completed(futures):
                f.result()

        self.metrics.stop()
        self._print_summary()

    def test_concurrent_writes(self):
        """并发写入文件测试"""
        print("\n" + "=" * 60)
        print("并发写入文件测试")
        print("=" * 60)

        self.metrics.start()

        num_files = TEST_CONFIG["num_files"]
        num_threads = TEST_CONFIG["num_threads"]
        test_dir = self.test_dirs[0]

        print(f"  写入 {num_files} 个文件，使用 {num_threads} 个线程...")

        def write_batch(thread_id: int):
            for i in range(thread_id, num_files, num_threads):
                file_name = f"stress-file-{i}.txt"
                content = f"Updated content from thread {thread_id}, file {i}, random: {random.randint(0, 1000)}"
                self.write_file(test_dir, file_name, content)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(write_batch, t) for t in range(num_threads)]
            for f in as_completed(futures):
                f.result()

        self.metrics.stop()
        self._print_summary()

    def test_concurrent_mixed(self):
        """混合操作并发测试"""
        print("\n" + "=" * 60)
        print("混合操作并发测试")
        print("=" * 60)

        self.metrics.start()

        num_threads = TEST_CONFIG["num_threads"]
        operations_per_thread = TEST_CONFIG["operations_per_thread"]
        test_dir = self.test_dirs[0]

        print(f"  {num_threads} 个线程，每个执行 {operations_per_thread} 个混合操作...")

        def mixed_operations(thread_id: int):
            test_dir_alt = self.test_dirs[thread_id % len(self.test_dirs)]

            for i in range(operations_per_thread):
                op = random.choice(["create", "read", "write", "delete", "list"])
                file_index = random.randint(0, TEST_CONFIG["num_files"] - 1)
                file_name = f"mixed-file-{file_index}.txt"

                if op == "create":
                    content = f"Created by thread {thread_id}, op {i}"
                    self.create_file(test_dir_alt, file_name, content)
                elif op == "read":
                    self.read_file(test_dir_alt, file_name)
                elif op == "write":
                    content = f"Written by thread {thread_id}, op {i}"
                    self.write_file(test_dir_alt, file_name, content)
                elif op == "delete":
                    self.delete_file(test_dir_alt, file_name)
                elif op == "list":
                    self.list_dir(test_dir_alt)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(mixed_operations, t) for t in range(num_threads)]
            for f in as_completed(futures):
                f.result()

        self.metrics.stop()
        self._print_summary()

    def test_cross_client_conflict(self):
        """跨客户端冲突测试"""
        print("\n" + "=" * 60)
        print("跨客户端冲突测试")
        print("=" * 60)

        self.metrics.start()

        num_threads = TEST_CONFIG["num_threads"]
        test_dir_1 = self.test_dirs[0]
        test_dir_2 = self.test_dirs[1] if len(self.test_dirs) > 1 else self.test_dirs[0]

        print(f"  两个挂载点并发操作，{num_threads} 个线程...")

        # 预置一些文件
        for i in range(100):
            self.create_file(test_dir_1, f"conflict-file-{i}.txt", f"Initial content {i}")

        time.sleep(1)

        def conflict_operations(thread_id: int):
            for i in range(TEST_CONFIG["operations_per_thread"]):
                file_index = random.randint(0, 99)
                file_name = f"conflict-file-{file_index}.txt"

                # 在两个目录间交替操作
                if i % 2 == 0:
                    # 客户端1 写入
                    content = f"Client1 update: thread {thread_id}, op {i}"
                    self.write_file(test_dir_1, file_name, content)
                else:
                    # 客户端2 写入
                    content = f"Client2 update: thread {thread_id}, op {i}"
                    self.write_file(test_dir_2, file_name, content)

                # 读取验证
                self.read_file(test_dir_1, file_name)
                self.read_file(test_dir_2, file_name)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(conflict_operations, t) for t in range(num_threads)]
            for f in as_completed(futures):
                f.result()

        self.metrics.stop()
        self._print_summary()

    # ========================================================================
    # 结果输出
    # ========================================================================

    def _print_summary(self):
        """打印测试摘要"""
        summary = self.metrics.get_summary()

        print(f"\n  测试摘要:")
        print(f"    总操作数: {summary['total_operations']}")
        print(f"    成功: {summary['successful']} ({summary['success_rate']:.1f}%)")
        print(f"    失败: {summary['failed']}")
        print(f"    持续时间: {summary['duration_sec']:.2f}s")
        print(f"    吞吐量: {summary['throughput_ops_per_sec']:.1f} ops/s")

        if summary.get("latency"):
            lat = summary["latency"]
            print(f"\n  延迟统计:")
            print(f"    最小: {lat['min_ms']:.2f}ms")
            print(f"    最大: {lat['max_ms']:.2f}ms")
            print(f"    平均: {lat['avg_ms']:.2f}ms")
            print(f"    P50:  {lat['p50_ms']:.2f}ms")
            print(f"    P95:  {lat['p95_ms']:.2f}ms")
            print(f"    P99:  {lat['p99_ms']:.2f}ms")

        if summary.get("operations_by_type"):
            print(f"\n  操作类型统计:")
            for op_type, stats in summary["operations_by_type"].items():
                print(f"    {op_type}: {stats['count']} 次, avg={stats['avg_ms']:.2f}ms, p99={stats['p99_ms']:.2f}ms")

        if self.metrics.errors:
            print(f"\n  错误 (显示前10条):")
            for err in self.metrics.errors[:10]:
                print(f"    - {err['error']}")

    def run_all_tests(self):
        """运行所有压力测试"""
        print("=" * 60)
        print("PowerFS CRDT 压力测试")
        print("=" * 60)
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"配置: {json.dumps(TEST_CONFIG, indent=2)}")
        print()

        # 检查挂载点
        for mount in MOUNT_POINTS:
            if not os.path.ismount(mount):
                print(f"错误: {mount} 未挂载")
                return False

        # 设置
        self.setup()

        # 运行测试
        try:
            # 1. 并发创建
            self.test_concurrent_creates()

            # 2. 并发读取
            self.test_concurrent_reads()

            # 3. 并发写入
            self.test_concurrent_writes()

            # 4. 混合操作
            self.test_concurrent_mixed()

            # 5. 跨客户端冲突
            self.test_cross_client_conflict()

        finally:
            # 清理
            self.cleanup()

        return True


def main():
    """主入口"""
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("用法: python crdt_stress_test.py")
        print()
        print("环境要求:")
        print("  - PowerFS CRDT 测试环境已启动")
        print("  - FUSE 客户端已挂载到:")
        for mount in MOUNT_POINTS:
            print(f"    {mount}")
        print()
        print("测试场景:")
        print("  1. 并发创建文件")
        print("  2. 并发读取文件")
        print("  3. 并发写入文件")
        print("  4. 混合操作并发")
        print("  5. 跨客户端冲突")
        print()
        print("配置:")
        print("  --num-files N        测试文件数量 (默认: 1000)")
        print("  --num-threads N      并发线程数 (默认: 10)")
        print("  --ops-per-thread N   每线程操作数 (默认: 100)")
        return

    # 解析命令行参数
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--num-files" and i + 1 < len(sys.argv):
            TEST_CONFIG["num_files"] = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == "--num-threads" and i + 1 < len(sys.argv):
            TEST_CONFIG["num_threads"] = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == "--ops-per-thread" and i + 1 < len(sys.argv):
            TEST_CONFIG["operations_per_thread"] = int(sys.argv[i + 1])
            i += 2
        else:
            i += 1

    # 运行测试
    test = PowerFSStressTest()
    success = test.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
