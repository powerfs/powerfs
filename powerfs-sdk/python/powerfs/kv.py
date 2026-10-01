"""High-level Python client for PowerFS KV.

This module provides:

* :class:`KVClient` — Mooncake-style byte KV + PyTorch tensor storage plus the
  PagedAttention session/block API, on top of the gRPC transport in
  :mod:`powerfs.client`.
* :class:`KVAdminClient` — namespace management and statistics.

Return-code convention (kept for Mooncake API compatibility):
``0`` success, ``-1`` generic error, ``-2`` not found, ``-3`` permission
denied.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

from powerfs.client import KVCacheClient, KVError, normalize_masters

# Tensor metadata (dtype/shape) is stored alongside the raw bytes under this
# companion key, so values remain raw-byte compatible with the CLI and Rust
# SDK. Example: key "w" -> bytes, key "w.pymeta" -> JSON header.
META_SUFFIX = ".pymeta"


class ReplicateConfig:
    """Mooncake-style replication options.

    Note: replication topology is currently controlled by the collection the
    session belongs to; these fields are accepted for API compatibility but
    do not change per-key placement yet.
    """

    def __init__(self) -> None:
        self.replica_num = 1
        self.with_soft_pin = False
        self.preferred_segment = ""


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------


def _is_success(resp: Any) -> bool:
    return bool(getattr(resp, "success", False))


def _err_code(err: str) -> int:
    low = (err or "").lower()
    if "not found" in low:
        return -2
    if "permission" in low:
        return -3
    return -1


# ----------------------------------------------------------------------
# data client
# ----------------------------------------------------------------------


class KVClient:
    """High-level KV client bound to a namespace.

    Typical usage::

        client = KVClient()
        client.connect("10.0.0.1:9333,10.0.0.2:9333", namespace="default")
        client.put("config", b'{"model": "llama"}')
        code, value = client.get("config")
    """

    def __init__(self) -> None:
        self._grpc = KVCacheClient()
        self.namespace: str = "default"
        self.owner_id: str = ""

    # ------------------------------------------------------------------
    # connection
    # ------------------------------------------------------------------

    def connect(
        self,
        masters: str | Sequence[str],
        namespace: str = "default",
        owner_id: str = "",
        timeout: float = 10.0,
    ) -> int:
        """Connect to a PowerFS cluster (one address or a comma-separated
        list). Returns ``0`` on success, ``-1`` on failure."""
        try:
            self._grpc.connect(masters, timeout=timeout)
        except (KVError, ValueError):
            return -1
        self.namespace = namespace
        self.owner_id = owner_id
        return 0

    def setup(
        self,
        local_hostname: str,
        metadata_server: str,
        global_segment_size: int,
        local_buffer_size: int,
        protocol: str = "tcp",
        rdma_devices: str = "",
        master_server_address: str = "",
    ) -> int:
        """Mooncake-compatible initializer.

        Master gRPC addresses are taken from ``master_server_address`` (or
        ``metadata_server`` as fallback; a comma-separated list is accepted).
        The buffer-size / RDMA arguments are accepted for API compatibility.
        """
        masters = master_server_address or metadata_server
        if not masters:
            return -1
        return self.connect(masters)

    def close(self) -> int:
        self._grpc.close()
        return 0

    @property
    def leader(self) -> Optional[str]:
        return self._grpc.leader

    def __enter__(self) -> "KVClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # byte KV
    # ------------------------------------------------------------------

    def put(
        self, key: str, value: bytes, config: Optional[ReplicateConfig] = None
    ) -> int:
        try:
            resp = self._grpc.kv_put(
                self.namespace, key, value, owner_id=self.owner_id
            )
        except KVError:
            return -1
        return 0 if _is_success(resp) else _err_code(resp.error)

    def get(self, key: str) -> Tuple[int, Optional[bytes]]:
        try:
            resp = self._grpc.kv_get_raw(self.namespace, key)
        except KVError:
            return -1, None

        if not resp.success:
            return _err_code(resp.error), None
        if not resp.found:
            return -2, None
        return 0, resp.value

    def put_batch(
        self, keys: Sequence[str], values: Sequence[bytes]
    ) -> List[int]:
        if len(keys) != len(values):
            return [-1] * len(keys)
        try:
            resp = self._grpc.kv_batch_put(
                self.namespace, keys, values, owner_id=self.owner_id
            )
        except KVError:
            return [-1] * len(keys)
        if resp.error:
            return [-1] * len(keys)
        return [0 if ok else -1 for ok in resp.successes]

    def get_batch(
        self, keys: Sequence[str]
    ) -> Tuple[int, List[Optional[bytes]]]:
        try:
            resp = self._grpc.kv_batch_get(self.namespace, keys)
        except KVError:
            return -1, [None] * len(keys)
        if resp.error:
            return -1, [None] * len(keys)
        values: List[Optional[bytes]] = [
            v if f else None for v, f in zip(resp.values, resp.found)
        ]
        return 0, values

    def is_exist(self, key: str) -> int:
        try:
            resp = self._grpc.kv_exists(self.namespace, key)
        except KVError:
            return -1
        if resp.error:
            return _err_code(resp.error)
        return 1 if resp.exists else 0

    def remove(self, key: str) -> int:
        """Delete a key; its tensor metadata companion (if any) is also
        removed."""
        try:
            resp = self._grpc.kv_delete(self.namespace, key)
            if not resp.success:
                return _err_code(resp.error)
            meta_resp = self._grpc.kv_delete(self.namespace, key + META_SUFFIX)
            # Missing meta is the normal case; ignore its result.
            _ = meta_resp
            return 0
        except KVError:
            return -1

    def remove_by_regex(self, pattern: str) -> int:
        try:
            resp = self._grpc.kv_remove_by_regex(self.namespace, pattern)
        except KVError:
            return -1
        return 0 if _is_success(resp) else _err_code(resp.error)

    def remove_all(self) -> int:
        try:
            resp = self._grpc.kv_remove_all(self.namespace)
        except KVError:
            return -1
        return 0 if _is_success(resp) else _err_code(resp.error)

    def list_keys(self, prefix: Optional[str] = None) -> Tuple[int, List[str]]:
        try:
            resp = self._grpc.kv_list(self.namespace, prefix or "")
        except KVError:
            return -1, []
        if resp.error:
            return _err_code(resp.error), []
        # Hide internal tensor-meta companions from user-facing listings.
        return 0, [k for k in resp.keys if not k.endswith(META_SUFFIX)]

    # ------------------------------------------------------------------
    # PagedAttention: sessions and blocks
    # ------------------------------------------------------------------

    def create_session(
        self,
        session_id: str,
        model_name: str,
        num_layers: int,
        num_heads: int,
        head_dim: int,
        dtype: str = "fp16",
        ttl_seconds: int = 0,
        collection: str = "",
    ) -> int:
        try:
            resp = self._grpc.create_session(
                session_id,
                model_name,
                num_layers,
                num_heads,
                head_dim,
                dtype=dtype,
                ttl_seconds=ttl_seconds,
                owner_id=self.owner_id,
                namespace_id=self.namespace,
                collection=collection,
            )
        except KVError:
            return -1
        return 0 if resp.success else _err_code(resp.error)

    def delete_session(self, session_id: str) -> int:
        try:
            resp = self._grpc.delete_session(session_id)
        except KVError:
            return -1
        return 0 if resp.success else _err_code(resp.error)

    def get_session(self, session_id: str) -> Dict[str, Any]:
        """Return a session summary dict, or an empty dict if not found."""
        try:
            resp = self._grpc.get_session(session_id)
        except KVError:
            return {}
        if not resp.exists:
            return {}
        return {
            "session_id": resp.session_id,
            "model_name": resp.model_name,
            "num_layers": resp.num_layers,
            "num_blocks": resp.num_blocks,
            "total_tokens": resp.total_tokens,
            "used_bytes": resp.used_bytes,
        }

    def list_sessions(self, prefix: str = "") -> List[str]:
        try:
            resp = self._grpc.list_sessions(prefix=prefix)
        except KVError:
            return []
        return list(resp.session_ids)

    def put_block(
        self,
        session_id: str,
        layer_id: int,
        num_tokens: int,
        data: bytes,
    ) -> int:
        """Store one KV block; returns the block id, or ``-1`` on failure."""
        try:
            resp = self._grpc.put_block(
                session_id, layer_id, num_tokens, data
            )
        except KVError:
            return -1
        return int(resp.block_id) if resp.success else -1

    def get_block(self, block_id: int) -> Optional[Dict[str, Any]]:
        """Return ``{block_id, layer_id, num_tokens, fid, data}`` or None."""
        try:
            resp = self._grpc.get_block(block_id)
        except KVError:
            return None
        if not resp.found:
            return None
        return {
            "block_id": resp.block_id,
            "layer_id": resp.layer_id,
            "num_tokens": resp.num_tokens,
            "fid": resp.fid,
            "data": resp.data,
        }

    def batch_put_blocks(
        self, blocks: Sequence[Tuple[str, int, int, bytes]]
    ) -> List[int]:
        """Each item: ``(session_id, layer_id, num_tokens, data)``.

        Returns block ids (``-1`` for failed items).
        """
        from powerfs.proto.master_pb2 import PutBlockRequest

        reqs = [
            PutBlockRequest(
                session_id=sid,
                layer_id=layer,
                num_tokens=tokens,
                data=data,
            )
            for sid, layer, tokens, data in blocks
        ]
        try:
            resp = self._grpc.batch_put_blocks(reqs)
        except KVError:
            return [-1] * len(reqs)
        return [
            int(r.block_id) if r.success else -1 for r in resp.results
        ]

    def batch_get_blocks(
        self, block_ids: Sequence[int]
    ) -> List[Optional[Dict[str, Any]]]:
        try:
            resp = self._grpc.batch_get_blocks(block_ids)
        except KVError:
            return [None] * len(block_ids)
        out: List[Optional[Dict[str, Any]]] = []
        for r in resp.blocks:
            if not r.found:
                out.append(None)
            else:
                out.append(
                    {
                        "block_id": r.block_id,
                        "layer_id": r.layer_id,
                        "num_tokens": r.num_tokens,
                        "fid": r.fid,
                        "data": r.data,
                    }
                )
        return out

    # ------------------------------------------------------------------
    # PyTorch tensors
    # ------------------------------------------------------------------

    def put_tensor(
        self,
        key: str,
        tensor: Any,
        config: Optional[ReplicateConfig] = None,
    ) -> int:
        """Store a torch tensor (any device): raw bytes + dtype/shape
        metadata, so :meth:`get_tensor` restores shape and dtype."""
        try:
            import torch
        except ImportError:
            return -2

        if not isinstance(tensor, torch.Tensor):
            return -1

        cpu = tensor.detach().contiguous().cpu()
        # numpy (even 2.0) cannot export bfloat16; view as int16 to preserve
        # the exact bits; get_tensor reconstructs with dtype=bfloat16.
        if cpu.dtype == torch.bfloat16:
            raw = cpu.view(torch.int16).numpy().tobytes()
        else:
            raw = cpu.numpy().tobytes()
        header = json.dumps(
            {"dtype": str(tensor.dtype).replace("torch.", ""),
             "shape": list(tensor.shape)}
        ).encode()

        if self.put(key, raw) != 0:
            return -1
        if self.put(key + META_SUFFIX, header) != 0:
            return -1
        return 0

    def get_tensor(self, key: str) -> Tuple[int, Optional[Any]]:
        """Retrieve a tensor with original dtype/shape.

        If no metadata is present (e.g. the value was written by the CLI), a
        flat 1-D float32 view of the bytes is returned.
        """
        try:
            import torch
        except ImportError:
            return -2, None

        code, raw = self.get(key)
        if code != 0 or raw is None:
            return code, None

        _, header_bytes = self.get(key + META_SUFFIX)
        dtype = torch.float32
        shape: Optional[List[int]] = None
        if header_bytes is not None:
            try:
                header = json.loads(header_bytes.decode())
                dtype = getattr(torch, str(header["dtype"]))
                shape = [int(x) for x in header["shape"]]
            except (ValueError, KeyError, AttributeError):
                pass

        # torch.frombuffer needs a writable buffer; bytearray(raw) is one.
        # element_size check gives a clean error instead of a RuntimeError.
        if len(raw) % dtype.itemsize != 0:
            return -1, None
        try:
            tensor = torch.frombuffer(bytearray(raw), dtype=dtype)
        except (RuntimeError, TypeError):
            return -1, None

        if shape is not None:
            tensor = tensor.reshape(shape)
        return 0, tensor

    def batch_put_tensor(
        self, keys: Sequence[str], tensors: Sequence[Any]
    ) -> List[int]:
        if len(keys) != len(tensors):
            return [-1] * len(keys)
        return [self.put_tensor(k, t) for k, t in zip(keys, tensors)]

    def batch_get_tensor(
        self, keys: Sequence[str]
    ) -> List[Optional[Any]]:
        out: List[Optional[Any]] = []
        for key in keys:
            code, tensor = self.get_tensor(key)
            out.append(tensor if code == 0 else None)
        return out

    def put_tensor_with_tp(
        self,
        key: str,
        tensor: Any,
        tp_rank: int,
        tp_size: int,
        split_dim: int = 0,
    ) -> int:
        """Store this rank's shard of a tensor (tensor-parallel).

        The shard is ``narrow(split_dim)`` to an even slice and stored under
        ``"{key}_tp{rank}"`` with its own dtype/shape metadata.
        """
        try:
            import torch
        except ImportError:
            return -2

        if not isinstance(tensor, torch.Tensor):
            return -1

        if tp_size == 1:
            return self.put_tensor(key, tensor)

        dim_size = tensor.shape[split_dim] // tp_size
        start = tp_rank * dim_size
        end = start + dim_size
        if tp_rank == tp_size - 1:
            end = tensor.shape[split_dim]

        shard = torch.narrow(tensor, split_dim, start, end - start)
        return self.put_tensor(f"{key}_tp{tp_rank}", shard)

    def get_tensor_with_tp(
        self, key: str, tp_rank: int, tp_size: int
    ) -> Tuple[int, Optional[Any]]:
        """Retrieve this rank's shard previously stored via
        :meth:`put_tensor_with_tp`."""
        if tp_size == 1:
            return self.get_tensor(key)
        return self.get_tensor(f"{key}_tp{tp_rank}")

    # ------------------------------------------------------------------
    # stats
    # ------------------------------------------------------------------

    def stats(self) -> Dict[str, int]:
        try:
            resp = self._grpc.get_stats()
        except KVError:
            return {}
        return {
            "total_sessions": resp.total_sessions,
            "total_blocks": resp.total_blocks,
            "used_memory_bytes": resp.used_memory_bytes,
            "max_memory_bytes": resp.max_memory_bytes,
            "cache_hits": resp.cache_hits,
            "cache_misses": resp.cache_misses,
            "evictions": resp.evictions,
        }


# ----------------------------------------------------------------------
# admin client
# ----------------------------------------------------------------------


class KVAdminClient:
    """Namespace management and statistics over gRPC.

    Note: API-key creation/listing is a monitor (Web console) feature and is
    intentionally not part of this client; use ``powerfs-cli`` or the monitor
    for API keys.
    """

    def __init__(self) -> None:
        self._grpc = KVCacheClient()

    def connect(
        self, masters: str | Sequence[str], timeout: float = 10.0
    ) -> int:
        try:
            self._grpc.connect(masters, timeout=timeout)
        except (KVError, ValueError):
            return -1
        return 0

    def setup(self, base_url: str, token: Optional[str] = None) -> int:
        """Compatibility initializer; ``base_url`` is a master address (or a
        comma-separated list) and ``token`` is ignored."""
        try:
            normalize_masters(base_url)
        except ValueError:
            return -1
        return self.connect(base_url)

    def close(self) -> int:
        self._grpc.close()
        return 0

    def create_namespace(
        self, namespace_id: str, name: str, owner_id: str = ""
    ) -> Tuple[int, Dict[str, Any]]:
        try:
            resp = self._grpc.create_namespace(
                namespace_id, name, owner_id
            )
        except KVError:
            return -1, {}
        if not resp.success:
            return _err_code(resp.error), {}
        return 0, {"id": resp.namespace_id}

    def list_namespaces(self) -> Tuple[int, List[Dict[str, Any]]]:
        try:
            resp = self._grpc.list_namespaces()
        except KVError:
            return -1, []
        if resp.error:
            return _err_code(resp.error), []
        return 0, [_namespace_dict(ns) for ns in resp.namespaces]

    def get_namespace(
        self, namespace_id: str
    ) -> Tuple[int, Dict[str, Any]]:
        try:
            resp = self._grpc.get_namespace(namespace_id)
        except KVError:
            return -1, {}
        if resp.error:
            return _err_code(resp.error), {}
        if not resp.found:
            return -2, {}
        return 0, _namespace_dict(resp.namespace)

    def delete_namespace(
        self, namespace_id: str, owner_id: str = ""
    ) -> int:
        try:
            resp = self._grpc.delete_namespace(namespace_id, owner_id)
        except KVError:
            return -1
        return 0 if resp.success else _err_code(resp.error)

    def get_stats(self) -> Tuple[int, Dict[str, Any]]:
        try:
            resp = self._grpc.get_stats()
        except KVError:
            return -1, {}
        return 0, {
            "total_sessions": resp.total_sessions,
            "total_blocks": resp.total_blocks,
            "used_memory_bytes": resp.used_memory_bytes,
            "max_memory_bytes": resp.max_memory_bytes,
            "cache_hits": resp.cache_hits,
            "cache_misses": resp.cache_misses,
            "evictions": resp.evictions,
        }


def _namespace_dict(ns: Any) -> Dict[str, Any]:
    return {
        "id": ns.id,
        "name": ns.name,
        "owner_id": ns.owner_id,
        "created_at": ns.created_at,
        "updated_at": ns.updated_at,
    }
