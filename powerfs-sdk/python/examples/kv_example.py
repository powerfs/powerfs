#!/usr/bin/env python3
"""
PowerFS KV Python SDK 使用示例（Mooncake 兼容）

请先安装依赖：
pip install requests numpy torch

使用前请确保：
1. PowerFS 服务已启动
2. 已通过 Web 界面创建 API Key（Access Key 和 Secret Key）
3. 已创建命名空间（可选）
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from powerfs import KVClient, KVAdminClient, ReplicateConfig


def example_pattern_1_simple_kv_store():
    print("=== Pattern 1: Simple KV Store ===")

    store = KVClient()
    result = store.setup("localhost", "http://localhost:8080",
                        512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")
    if result != 0:
        print(f"Failed to setup store: {result}")
        return

    store.access_key = "your-access-key"
    store.secret_key = "your-secret-key"
    store.namespace = "default"

    key = "config"
    value = b'{"model": "llama-7b"}'

    result = store.put(key, value)
    if result != 0:
        print(f"Put failed with error code: {result}")
    else:
        print(f"Put successful")

    result, data = store.get(key)
    if result == 0 and data:
        print(f"Get successful: {data.decode()}")
    else:
        print(f"Get failed with error code: {result}")

    exists = store.is_exist(key)
    if exists == 1:
        print("Key exists")
    elif exists == 0:
        print("Key not found")
    else:
        print("Error checking existence")

    result = store.remove(key)
    if result == 0:
        print("Remove successful")
    else:
        print(f"Remove failed with error code: {result}")

    store.close()


def example_pattern_2_high_performance_tensor():
    print("\n=== Pattern 2: High-Performance Tensor Storage ===")

    try:
        import torch
    except ImportError:
        print("PyTorch not installed, skipping tensor example")
        return

    store = KVClient()
    result = store.setup("localhost", "http://localhost:8080",
                        512*1024*1024, 128*1024*1024, "rdma", "mlx5_0", "localhost:50051")
    if result != 0:
        print(f"Failed to setup store: {result}")
        return

    store.access_key = "your-access-key"
    store.secret_key = "your-secret-key"

    config = ReplicateConfig()
    config.replica_num = 2
    config.with_soft_pin = True

    tensor = torch.randn(1000, 1000)
    result = store.put_tensor("weights", tensor, config)
    if result == 0:
        print("Put tensor successful")
    else:
        print(f"Put tensor failed with error code: {result}")

    result, retrieved = store.get_tensor("weights")
    if result == 0 and retrieved is not None:
        print(f"Get tensor successful, shape: {retrieved.shape}")
    else:
        print(f"Get tensor failed with error code: {result}")

    store.close()


def example_pattern_3_zero_copy_batch():
    print("\n=== Pattern 3: Zero-Copy Batch Operations ===")

    import numpy as np

    store = KVClient()
    result = store.setup("localhost", "http://localhost:8080",
                        512*1024*1024, 16*1024*1024, "rdma", "", "localhost:50051")
    if result != 0:
        print(f"Failed to setup store: {result}")
        return

    store.access_key = "your-access-key"
    store.secret_key = "your-secret-key"

    num_buffers = 3
    buffers = [np.random.randn(1024*1024).astype(np.float32) for _ in range(num_buffers)]
    buffer_ptrs = [buf.ctypes.data for buf in buffers]
    sizes = [buf.nbytes for buf in buffers]

    for ptr, size in zip(buffer_ptrs, sizes):
        result = store.register_buffer(ptr, size)
        if result != 0:
            print(f"Failed to register buffer: {result}")

    keys = [f"tensor_{i}" for i in range(num_buffers)]
    results = store.batch_put_from(keys, buffer_ptrs, sizes)
    for i, r in enumerate(results):
        if r == 0:
            print(f"Batch put {i} successful")
        else:
            print(f"Batch put {i} failed with error code: {r}")

    recv_buffers = [np.empty(1024*1024, dtype=np.float32) for _ in range(num_buffers)]
    recv_ptrs = [buf.ctypes.data for buf in recv_buffers]

    for ptr, size in zip(recv_ptrs, sizes):
        store.register_buffer(ptr, size)

    results = store.batch_get_into(keys, recv_ptrs, sizes)
    for i, r in enumerate(results):
        if r == 0:
            print(f"Batch get {i} successful")
        else:
            print(f"Batch get {i} failed with error code: {r}")

    for ptr in buffer_ptrs + recv_ptrs:
        store.unregister_buffer(ptr)

    store.close()


def example_pattern_4_transfer_engine():
    print("\n=== Pattern 4: Transfer Engine Direct Transfer ===")
    print("Transfer Engine requires direct RDMA/TCP connections between nodes")
    print("This example demonstrates the conceptual usage pattern")

    try:
        from mooncake.engine import TransferEngine
        engine = TransferEngine()
        result = engine.initialize("127.0.0.1:12345", "127.0.0.1:2379", "tcp", "")
        if result != 0:
            print(f"Failed to initialize engine: {result}")
            return

        buffer = np.ones(1024*1024, dtype=np.uint8)
        buffer_ptr = buffer.ctypes.data
        result = engine.register_memory(buffer_ptr, buffer.nbytes)
        if result != 0:
            print(f"Failed to register memory: {result}")
            return

        print("Transfer Engine setup complete")
        print("Use engine.transfer_sync_write/read for actual data transfers")

        engine.unregister_memory(buffer_ptr)
    except ImportError:
        print("Mooncake Transfer Engine not available, skipping this example")
        print("Note: Transfer Engine requires the mooncake-engine Rust extension")


def example_batch_operations():
    print("\n=== Batch Operations ===")

    store = KVClient()
    result = store.setup("localhost", "http://localhost:8080",
                        512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")
    if result != 0:
        print(f"Failed to setup store: {result}")
        return

    store.access_key = "your-access-key"
    store.secret_key = "your-secret-key"

    keys = ["key1", "key2", "key3"]
    values = [b"value1", b"value2", b"value3"]

    results = store.put_batch(keys, values)
    for i, r in enumerate(results):
        if r == 0:
            print(f"Put {keys[i]} successful")
        else:
            print(f"Put {keys[i]} failed with error code: {r}")

    result, values = store.get_batch(keys)
    if result == 0:
        for i, v in enumerate(values):
            if v is not None:
                print(f"Get {keys[i]}: {v.decode()}")
            else:
                print(f"Get {keys[i]}: not found")
    else:
        print(f"Batch get failed with error code: {result}")

    store.close()


def example_tensor_parallelism():
    print("\n=== Tensor Parallelism ===")

    try:
        import torch
    except ImportError:
        print("PyTorch not installed, skipping TP example")
        return

    store = KVClient()
    result = store.setup("localhost", "http://localhost:8080",
                        512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")
    if result != 0:
        print(f"Failed to setup store: {result}")
        return

    store.access_key = "your-access-key"
    store.secret_key = "your-secret-key"

    tensor = torch.randn(4, 100)
    tp_size = 4
    split_dim = 0

    for rank in range(tp_size):
        result = store.put_tensor_with_tp("model_weights", tensor, rank, tp_size, split_dim)
        if result == 0:
            print(f"Put TP shard {rank} successful")
        else:
            print(f"Put TP shard {rank} failed with error code: {result}")

    for rank in range(tp_size):
        result, shard = store.get_tensor_with_tp("model_weights", rank, tp_size)
        if result == 0 and shard is not None:
            print(f"Get TP shard {rank} successful, shape: {shard.shape}")
        else:
            print(f"Get TP shard {rank} failed with error code: {result}")

    store.close()


def example_admin_operations():
    print("\n=== Admin Operations ===")

    admin = KVAdminClient()
    admin.setup("http://localhost:8080", "your-jwt-token")

    result, namespaces = admin.list_namespaces()
    if result == 0:
        print("Namespaces:")
        for ns in namespaces:
            print(f"  - {ns.get('name', 'N/A')} (ID: {ns.get('id', 'N/A')})")
    else:
        print(f"List namespaces failed with error code: {result}")

    result, metrics = admin.get_metrics()
    if result == 0:
        print("\nMetrics:")
        print(f"  Session count: {metrics.get('session_count', 0)}")
        print(f"  Block count: {metrics.get('block_count', 0)}")
        print(f"  Hit ratio: {metrics.get('hit_ratio', 0)}%")
    else:
        print(f"Get metrics failed with error code: {result}")


if __name__ == "__main__":
    print("PowerFS KV Python SDK 使用示例（Mooncake 兼容）")
    print("=" * 60)

    example_pattern_1_simple_kv_store()
    example_pattern_2_high_performance_tensor()
    example_pattern_3_zero_copy_batch()
    example_pattern_4_transfer_engine()
    example_batch_operations()
    example_tensor_parallelism()
    example_admin_operations()

    print("\n" + "=" * 60)
    print("示例执行完成")
