#!/usr/bin/env python3
"""PowerFS KV Python SDK example (gRPC).

Install deps first:

    pip install grpcio protobuf numpy            # byte KV
    pip install torch                             # tensor interface (optional)

Run against a cluster (master gRPC port 9333):

    python kv_example.py 10.0.0.11:9333,10.0.0.12:9333,10.0.0.13:9333

A single master address works too; with multiple addresses the client
discovers and follows the Raft leader automatically.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from powerfs import KVAdminClient, KVClient  # noqa: E402

NAMESPACE = "ns-example"
OWNER = "example-user"


def main() -> int:
    masters = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9333"

    # ------------------------------------------------------------ admin
    admin = KVAdminClient()
    if admin.connect(masters) != 0:
        print(f"failed to connect to masters: {masters}")
        return 1
    print(f"connected; creating namespace {NAMESPACE}")
    code, info = admin.create_namespace(NAMESPACE, "Example namespace", OWNER)
    # pre-existing namespace from a previous run is fine
    if code != 0:
        print(f"create_namespace returned code={code}; continuing")

    # ------------------------------------------------- byte key-value
    client = KVClient()
    if client.connect(masters, namespace=NAMESPACE, owner_id=OWNER) != 0:
        print("failed to connect data client")
        return 1
    print(f"data client leader: {client.leader}")

    client.put("config", b'{"model": "llama-7b"}')
    code, value = client.get("config")
    print(f"get config -> {value}")

    client.put_batch(["k1", "k2"], [b"value1", b"value2"])
    code, values = client.get_batch(["k1", "k2", "missing"])
    print(f"batch get -> {values}")  # [b'value1', b'value2', None]

    print(f"exists k1 -> {client.is_exist('k1')}")       # 1
    print(f"exists no -> {client.is_exist('missing')}")  # 0
    code, keys = client.list_keys()
    print(f"keys -> {keys}")

    # ------------------------------------------------------ torch tensor
    try:
        import torch
    except ImportError:
        print("torch not installed; skipping tensor examples")
        torch = None

    if torch is not None:
        t = torch.randn(2, 3).to(torch.float16)
        client.put_tensor("weights", t)
        code, got = client.get_tensor("weights")
        print(f"tensor weights -> dtype={got.dtype}, shape={got.shape}, "
              f"equal={torch.equal(got, t)}")

        # tensor-parallel shards: each rank stores its own slice
        tp_size = 2
        big = torch.randn(4, 2)
        for rank in range(tp_size):
            client.put_tensor_with_tp(
                "model_w", big, rank, tp_size, split_dim=0
            )
        for rank in range(tp_size):
            code, shard = client.get_tensor_with_tp("model_w", rank, tp_size)
            print(f"tp shard rank={rank} -> shape={shard.shape}")

    # --------------------------- PagedAttention: session + raw blocks
    client.create_session(
        "sess-example",
        model_name="tiny-llm",
        num_layers=2,
        num_heads=4,
        head_dim=64,
        dtype="fp16",
        ttl_seconds=600,
    )
    data = os.urandom(4096)
    block_id = client.put_block("sess-example", layer_id=0, num_tokens=128, data=data)
    block = client.get_block(block_id)
    print(f"block {block_id} -> {len(block['data'])} bytes, fid={block['fid']}")

    print("stats:", client.stats())

    # -------------------------------------------------------- cleanup
    client.delete_session("sess-example")
    client.remove_all()
    admin.delete_namespace(NAMESPACE, OWNER)
    client.close()
    admin.close()
    print("done (namespace removed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
