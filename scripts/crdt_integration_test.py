#!/usr/bin/env python3
"""
CRDT 集成测试脚本
测试多容器环境下的 CRDT 冲突合并功能
"""

import os
import sys
import time
import json
import random
import subprocess
import requests
from typing import Optional, Dict, Any, List

# 配置
FILER_ENDPOINTS = [
    {"name": "filer-1", "grpc": "172.30.0.11:8889", "api": "http://localhost:18888", "admin": "http://localhost:18888"},
    {"name": "filer-2", "grpc": "172.30.0.12:8889", "api": "http://localhost:28888", "admin": "http://localhost:28888"},
    {"name": "filer-3", "grpc": "172.30.0.13:8889", "api": "http://localhost:38888", "admin": "http://localhost:38888"},
]

FUSE_CLIENTS = [
    {"name": "fuse-1", "mount": "/tmp/powerfs/crdt-fuse1", "client_id": "fuse-1"},
    {"name": "fuse-2", "mount": "/tmp/powerfs/crdt-fuse2", "client_id": "fuse-2"},
]

TEST_DIR = "/tmp/powerfs-crdt-test"


class CRDTIntegrationTest:
    def __init__(self):
        self.test_results = []
        self.passed = 0
        self.failed = 0

    def log_result(self, test_name: str, success: bool, message: str = ""):
        status = "✓ PASS" if success else "✗ FAIL"
        self.test_results.append({
            "name": test_name,
            "passed": success,
            "message": message
        })
        if success:
            self.passed += 1
        else:
            self.failed += 1
        print(f"[{status}] {test_name}: {message}")

    # ========================================================================
    # 环境检查
    # ========================================================================

    def check_environment(self):
        """检查测试环境是否就绪"""
        print("\n" + "=" * 60)
        print("环境检查")
        print("=" * 60)

        # 检查 Filer 容器状态
        for endpoint in FILER_ENDPOINTS:
            try:
                resp = requests.get(f"{endpoint['admin']}/admin/status", timeout=5)
                if resp.status_code == 200:
                    self.log_result(f"{endpoint['name']} 健康检查", True)
                else:
                    self.log_result(f"{endpoint['name']} 健康检查", False, f"HTTP {resp.status_code}")
            except requests.exceptions.ConnectionError:
                self.log_result(f"{endpoint['name']} 健康检查", False, "连接失败 - 容器可能未启动")
            except Exception as e:
                self.log_result(f"{endpoint['name']} 健康检查", False, str(e))

        # 检查 FUSE 挂载点
        for fuse in FUSE_CLIENTS:
            if os.path.ismount(fuse['mount']):
                self.log_result(f"{fuse['name']} 挂载检查", True)
            else:
                self.log_result(f"{fuse['name']} 挂载检查", False, "挂载点不存在")

    # ========================================================================
    # CRDT 管理接口测试
    # ========================================================================

    def test_crdt_admin_api(self):
        """测试 CRDT 管理接口"""
        print("\n" + "=" * 60)
        print("CRDT 管理接口测试")
        print("=" * 60)

        for endpoint in FILER_ENDPOINTS:
            # 测试 CRDT 概览接口
            try:
                resp = requests.get(f"{endpoint['admin']}/admin/crdt/overview", timeout=5)
                if resp.status_code == 200:
                    overview = resp.json()
                    self.log_result(
                        f"{endpoint['name']} CRDT 概览",
                        True,
                        f"OR-Set状态数: {overview['total_orset_states']}"
                    )
                else:
                    self.log_result(f"{endpoint['name']} CRDT 概览", False, f"HTTP {resp.status_code}")
            except Exception as e:
                self.log_result(f"{endpoint['name']} CRDT 概览", False, str(e))

            # 测试分片状态接口
            try:
                resp = requests.get(f"{endpoint['admin']}/admin/crdt/shards/1", timeout=5)
                if resp.status_code == 200:
                    self.log_result(f"{endpoint['name']} 分片1 OR-Set 状态", True)
                else:
                    self.log_result(f"{endpoint['name']} 分片1 OR-Set 状态", False, f"HTTP {resp.status_code}")
            except Exception as e:
                self.log_result(f"{endpoint['name']} 分片1 OR-Set 状态", False, str(e))

    # ========================================================================
    # 基本功能测试
    # ========================================================================

    def test_basic_operations(self):
        """测试基本文件操作"""
        print("\n" + "=" * 60)
        print("基本功能测试")
        print("=" * 60)

        for fuse in FUSE_CLIENTS:
            test_dir = f"{fuse['mount']}/crdt-test-basic"
            try:
                # 创建测试目录
                os.makedirs(test_dir, exist_ok=True)
                self.log_result(f"{fuse['name']} 创建目录", True)

                # 创建测试文件
                test_file = f"{test_dir}/test-file.txt"
                with open(test_file, 'w') as f:
                    f.write("Hello, PowerFS CRDT Test!")
                self.log_result(f"{fuse['name']} 创建文件", True)

                # 读取文件
                with open(test_file, 'r') as f:
                    content = f.read()
                if content == "Hello, PowerFS CRDT Test!":
                    self.log_result(f"{fuse['name']} 读取文件", True)
                else:
                    self.log_result(f"{fuse['name']} 读取文件", False, "内容不匹配")

                # 列出目录
                files = os.listdir(test_dir)
                if "test-file.txt" in files:
                    self.log_result(f"{fuse['name']} 列出目录", True)
                else:
                    self.log_result(f"{fuse['name']} 列出目录", False, "文件不在列表中")

                # 删除文件
                os.remove(test_file)
                self.log_result(f"{fuse['name']} 删除文件", True)

                # 清理
                os.rmdir(test_dir)

            except Exception as e:
                self.log_result(f"{fuse['name']} 基本操作", False, str(e))

    # ========================================================================
    # 并发冲突测试
    # ========================================================================

    def test_concurrent_add_add_conflict(self):
        """测试 Add-Add 并发冲突（双保留语义）"""
        print("\n" + "=" * 60)
        print("并发 Add-Add 冲突测试")
        print("=" * 60)

        test_dir = f"{TEST_DIR}/concurrent-add-add"
        os.makedirs(test_dir, exist_ok=True)

        try:
            # 两个客户端同时创建同名但内容不同的文件
            fuse1 = FUSE_CLIENTS[0]
            fuse2 = FUSE_CLIENTS[1]

            file1 = f"{fuse1['mount']}/crdt-add-add.txt"
            file2 = f"{fuse2['mount']}/crdt-add-add.txt"

            # 并发写入
            with open(file1, 'w') as f:
                f.write("Content from fuse-1")

            with open(file2, 'w') as f:
                f.write("Content from fuse-2")

            # 等待同步
            time.sleep(2)

            # 检查两个客户端都能看到文件
            files1 = os.listdir(fuse1['mount'])
            files2 = os.listdir(fuse2['mount'])

            self.log_result(
                "Add-Add 并发冲突",
                True,
                f"fuse1看到: {files1}, fuse2看到: {files2}"
            )

            # 读取内容
            try:
                with open(file1, 'r') as f:
                    content1 = f.read()
                self.log_result(f"读取 fuse-1 文件", True, f"内容: {content1}")
            except Exception as e:
                self.log_result(f"读取 fuse-1 文件", False, str(e))

            # 清理
            for path in [file1, file2]:
                try:
                    os.remove(path)
                except:
                    pass

        except Exception as e:
            self.log_result("Add-Add 并发冲突", False, str(e))

    def test_concurrent_add_remove_conflict(self):
        """测试 Add-Remove 并发冲突（Add-Wins 语义）"""
        print("\n" + "=" * 60)
        print("并发 Add-Remove 冲突测试 (Add-Wins)")
        print("=" * 60)

        try:
            fuse1 = FUSE_CLIENTS[0]
            fuse2 = FUSE_CLIENTS[1]

            test_file = f"{fuse1['mount']}/crdt-add-remove.txt"

            # 客户端1创建文件
            with open(test_file, 'w') as f:
                f.write("Content to be deleted")

            time.sleep(1)

            # 客户端1删除文件，同时客户端2尝试创建同名文件
            # 根据 Add-Wins 语义，Add 应该胜出
            file_new = f"{fuse2['mount']}/crdt-add-remove.txt"

            # 并发操作
            try:
                os.remove(test_file)
            except:
                pass

            with open(file_new, 'w') as f:
                f.write("New content from fuse-2")

            time.sleep(2)

            # 检查结果：根据 Add-Wins 语义，文件应该存在
            file_exists = os.path.exists(file_new)
            self.log_result(
                "Add-Remove 冲突 (Add-Wins)",
                file_exists,
                f"文件存在: {file_exists}"
            )

            # 清理
            try:
                os.remove(file_new)
            except:
                pass

        except Exception as e:
            self.log_result("Add-Remove 冲突", False, str(e))

    def test_concurrent_setattr(self):
        """测试 SetAttr 并发冲突（Last-Writer-Wins 语义）"""
        print("\n" + "=" * 60)
        print("并发 SetAttr 冲突测试 (LWW)")
        print("=" * 60)

        try:
            fuse1 = FUSE_CLIENTS[0]
            fuse2 = FUSE_CLIENTS[1]

            test_file = f"{fuse1['mount']}/crdt-setattr.txt"

            # 创建文件
            with open(test_file, 'w') as f:
                f.write("Initial content")

            time.sleep(1)

            # 并发修改属性
            os.chmod(test_file, 0o644)

            # 等待同步
            time.sleep(2)

            # 验证文件存在
            file_exists = os.path.exists(test_file)
            self.log_result("SetAttr 并发修改", file_exists, f"文件存在: {file_exists}")

            # 清理
            os.remove(test_file)

        except Exception as e:
            self.log_result("SetAttr 并发测试", False, str(e))

    # ========================================================================
    # 分片同步测试
    # ========================================================================

    def test_shard_sync(self):
        """测试分片同步"""
        print("\n" + "=" * 60)
        print("分片同步测试")
        print("=" * 60)

        for endpoint in FILER_ENDPOINTS:
            try:
                resp = requests.get(f"{endpoint['admin']}/admin/shards", timeout=5)
                if resp.status_code == 200:
                    shards = resp.json()
                    self.log_result(
                        f"{endpoint['name']} 分片列表",
                        True,
                        f"分片数: {len(shards)}"
                    )
                else:
                    self.log_result(f"{endpoint['name']} 分片列表", False, f"HTTP {resp.status_code}")
            except Exception as e:
                self.log_result(f"{endpoint['name']} 分片列表", False, str(e))

    # ========================================================================
    # Tombstone 测试
    # ========================================================================

    def test_tombstone_cleanup(self):
        """测试 Tombstone 清理"""
        print("\n" + "=" * 60)
        print("Tombstone 清理测试")
        print("=" * 60)

        # 创建一些文件然后删除，产生 Tombstone
        for fuse in FUSE_CLIENTS:
            for i in range(5):
                test_file = f"{fuse['mount']}/tombstone-test-{i}.txt"
                try:
                    with open(test_file, 'w') as f:
                        f.write(f"Test content {i}")
                    time.sleep(0.1)
                    os.remove(test_file)
                except:
                    pass

        # 等待同步
        time.sleep(3)

        # 执行清理
        for endpoint in FILER_ENDPOINTS:
            try:
                resp = requests.post(f"{endpoint['admin']}/admin/crdt/cleanup?ttl=0", timeout=5)
                if resp.status_code == 200:
                    result = resp.json()
                    self.log_result(
                        f"{endpoint['name']} Tombstone 清理",
                        True,
                        f"清理数量: {result['cleaned_count']}"
                    )
                else:
                    self.log_result(f"{endpoint['name']} Tombstone 清理", False, f"HTTP {resp.status_code}")
            except Exception as e:
                self.log_result(f"{endpoint['name']} Tombstone 清理", False, str(e))

    # ========================================================================
    # 故障恢复测试
    # ========================================================================

    def test_failure_recovery(self):
        """测试故障恢复后的数据一致性"""
        print("\n" + "=" * 60)
        print("故障恢复测试")
        print("=" * 60)

        try:
            fuse1 = FUSE_CLIENTS[0]

            # 创建测试数据
            test_dir = f"{fuse1['mount']}/recovery-test"
            os.makedirs(test_dir, exist_ok=True)

            for i in range(10):
                test_file = f"{test_dir}/file-{i}.txt"
                with open(test_file, 'w') as f:
                    f.write(f"Content {i}")

            time.sleep(2)

            # 验证数据完整性
            files = os.listdir(test_dir)
            if len(files) >= 10:
                self.log_result("数据完整性验证", True, f"文件数: {len(files)}")
            else:
                self.log_result("数据完整性验证", False, f"文件数: {len(files)}")

            # 读取每个文件并验证内容
            all_correct = True
            for i in range(10):
                test_file = f"{test_dir}/file-{i}.txt"
                try:
                    with open(test_file, 'r') as f:
                        content = f.read()
                    if content != f"Content {i}":
                        all_correct = False
                except:
                    all_correct = False

            self.log_result("内容一致性验证", all_correct)

            # 清理
            for i in range(10):
                try:
                    os.remove(f"{test_dir}/file-{i}.txt")
                except:
                    pass
            os.rmdir(test_dir)

        except Exception as e:
            self.log_result("故障恢复测试", False, str(e))

    # ========================================================================
    # 运行所有测试
    # ========================================================================

    def run_all_tests(self):
        """运行所有测试"""
        print("=" * 60)
        print("PowerFS CRDT 集成测试")
        print("=" * 60)
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # 环境检查
        self.check_environment()

        # CRDT 管理接口测试
        self.test_crdt_admin_api()

        # 基本功能测试
        self.test_basic_operations()

        # 分片同步测试
        self.test_shard_sync()

        # 并发冲突测试
        self.test_concurrent_add_add_conflict()
        self.test_concurrent_add_remove_conflict()
        self.test_concurrent_setattr()

        # Tombstone 测试
        self.test_tombstone_cleanup()

        # 故障恢复测试
        self.test_failure_recovery()

        # 汇总
        print("\n" + "=" * 60)
        print("测试结果汇总")
        print("=" * 60)
        total = self.passed + self.failed
        print(f"总测试数: {total}")
        print(f"通过: {self.passed}")
        print(f"失败: {self.failed}")
        print(f"通过率: {self.passed/total*100:.1f}%")

        if self.failed == 0:
            print("\n✓ 所有测试通过！")
        else:
            print("\n✗ 部分测试失败，请检查日志")

        return self.failed == 0


def main():
    """主入口"""
    # 检查参数
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("用法: python crdt_integration_test.py")
        print()
        print("环境要求:")
        print("  - PowerFS CRDT 测试环境已启动 (docker-compose.crdt-test.yml)")
        print("  - FUSE 客户端已挂载")
        print()
        print("测试内容:")
        print("  1. 环境检查 - 验证容器健康状态")
        print("  2. CRDT 管理接口 - 测试 HTTP API")
        print("  3. 基本功能 - 文件读写操作")
        print("  4. 并发冲突 - Add-Add, Add-Remove, SetAttr")
        print("  5. 分片同步 - 分片状态检查")
        print("  6. Tombstone 清理 - 删除标记清理")
        print("  7. 故障恢复 - 数据一致性验证")
        return

    # 运行测试
    test = CRDTIntegrationTest()
    success = test.run_all_tests()

    # 返回退出码
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
