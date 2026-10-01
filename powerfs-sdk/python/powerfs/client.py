"""Low-level gRPC client for PowerFS KVCacheService.

Covers all 22 KV RPCs plus cluster leader discovery. This module speaks the
wire protocol directly (protobuf in, protobuf out); high-level convenience
wrappers live in :mod:`powerfs.kv`.
"""

from __future__ import annotations

import re
import threading
from typing import Any, Dict, Iterable, List, Optional, Sequence

import grpc

from powerfs.proto import master_pb2 as pb
from powerfs.proto import master_pb2_grpc as pbg

# Server-side wording (master.rs/admin_api): "not leader; current leader is ..."
_NOT_LEADER_RE = re.compile(r"not.{0,8}leader", re.IGNORECASE)

# Python gRPC keeps the exact proto RPC identifiers (KVPut, BatchGet, ...),
# which are not derivable from snake_case due to KV/ID acronyms; map them.
_RPC_NAMES: Dict[str, str] = {
    "create_session": "CreateSession",
    "delete_session": "DeleteSession",
    "get_session": "GetSession",
    "list_sessions": "ListSessions",
    "put_block": "PutBlock",
    "get_block": "GetBlock",
    "batch_put": "BatchPut",
    "batch_get": "BatchGet",
    "get_stats": "GetStats",
    "create_namespace": "CreateNamespace",
    "list_namespaces": "ListNamespaces",
    "get_namespace": "GetNamespace",
    "delete_namespace": "DeleteNamespace",
    "k_v_put": "KVPut",
    "k_v_get": "KVGet",
    "k_v_delete": "KVDelete",
    "k_v_exists": "KVExists",
    "k_v_list": "KVList",
    "k_v_remove_by_regex": "KVRemoveByRegex",
    "k_v_remove_all": "KVRemoveAll",
    "k_v_batch_put": "KVBatchPut",
    "k_v_batch_get": "KVBatchGet",
}

# Blocks can be tens of MB; allow generous wire limits by default.
_DEFAULT_MAX_MSG = 256 * 1024 * 1024


class KVError(Exception):
    """Raised on persistent transport/protocol failures.

    ``code`` follows the Mooncake-style convention used across this SDK:
    0 success, -1 generic error, -2 not found, -3 permission denied.
    """

    def __init__(self, message: str, code: int = -1):
        super().__init__(message)
        self.code = code

    def __str__(self) -> str:
        return f"[code={self.code}] {super().__str__()}"


def normalize_masters(masters: str | Sequence[str]) -> List[str]:
    """Accept "h1:9333,h2:9333" or a sequence; strip schemes and whitespace."""
    if isinstance(masters, str):
        parts: Iterable[str] = masters.split(",")
    else:
        parts = masters

    out: List[str] = []
    for raw in parts:
        addr = raw.strip()
        if not addr:
            continue
        for scheme in ("powerfs://", "grpc://", "http://", "https://"):
            if addr.startswith(scheme):
                addr = addr[len(scheme):]
        addr = addr.rstrip("/")
        if ":" not in addr:
            addr = f"{addr}:9333"
        if addr not in out:
            out.append(addr)

    if not out:
        raise ValueError("no master addresses provided")
    return out


class KVCacheClient:
    """gRPC client with automatic Raft leader discovery and failover.

    Parameters
    ----------
    call_timeout:
        Default per-RPC deadline in seconds.
    """

    def __init__(
        self,
        call_timeout: float = 30.0,
        max_message_length: int = _DEFAULT_MAX_MSG,
    ) -> None:
        self.call_timeout = call_timeout
        self._max_msg = max_message_length

        self._masters: List[str] = []
        self._channels: Dict[str, grpc.Channel] = {}
        self._kv_stubs: Dict[str, pbg.KVCacheServiceStub] = {}
        self._master_stubs: Dict[str, pbg.MasterServiceStub] = {}

        self._leader_addr: Optional[str] = None
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # connection management
    # ------------------------------------------------------------------

    def connect(
        self,
        masters: str | Sequence[str],
        timeout: float = 10.0,
    ) -> "KVCacheClient":
        """Open channels and locate the current leader.

        Raises :class:`KVError` if no master is reachable / no leader found.
        """
        with self._lock:
            self._masters = normalize_masters(masters)
            for addr in self._masters:
                self._ensure_channel(addr)

            self._discover_leader(timeout=timeout)
            if self._leader_addr is None:
                raise KVError(
                    f"no raft leader found among {self._masters}", -1
                )
        return self

    def close(self) -> None:
        with self._lock:
            for ch in self._channels.values():
                try:
                    ch.close()
                except Exception:
                    pass
            self._channels.clear()
            self._kv_stubs.clear()
            self._master_stubs.clear()
            self._leader_addr = None

    def __enter__(self) -> "KVCacheClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @property
    def leader(self) -> Optional[str]:
        """Currently targeted master address (may trigger no I/O)."""
        return self._leader_addr

    @property
    def masters(self) -> List[str]:
        return list(self._masters)

    def _ensure_channel(self, addr: str) -> grpc.Channel:
        ch = self._channels.get(addr)
        if ch is None:
            options = [
                ("grpc.max_send_message_length", self._max_msg),
                ("grpc.max_receive_message_length", self._max_msg),
                ("grpc.enable_retries", 0),
            ]
            ch = grpc.insecure_channel(addr, options=options)
            self._channels[addr] = ch
            self._kv_stubs[addr] = pbg.KVCacheServiceStub(ch)
            self._master_stubs[addr] = pbg.MasterServiceStub(ch)
        return ch

    # ------------------------------------------------------------------
    # leader discovery
    # ------------------------------------------------------------------

    def _discover_leader(self, timeout: float = 10.0) -> Optional[str]:
        found: Optional[str] = None

        for addr in list(self._masters):
            self._ensure_channel(addr)

            stub = self._master_stubs[addr]
            try:
                info = stub.GetClusterInfo(
                    pb.ClusterInfoRequest(), timeout=timeout
                )
            except grpc.RpcError:
                continue

            if info.is_leader:
                found = addr
                break

        self._leader_addr = found
        return found

    def refresh_leader(self, timeout: float = 10.0) -> str:
        """Force rediscovery; raise if the cluster has no usable leader."""
        with self._lock:
            addr = self._discover_leader(timeout=timeout)
            if addr is None:
                raise KVError("cannot determine current raft leader", -1)
            return addr

    # ------------------------------------------------------------------
    # core call machinery
    # ------------------------------------------------------------------

    def _kv_stub(self) -> pbg.KVCacheServiceStub:
        with self._lock:
            if not self._masters:
                raise KVError("client is not connected; call connect()", -1)

            addr = self._leader_addr
            if addr is None:
                addr = self.refresh_leader()
            return self._kv_stubs[addr]

    def call(
        self,
        rpc_name: str,
        request: Any,
        *,
        timeout: Optional[float] = None,
        retries: int = 2,
    ) -> Any:
        """Invoke a KVCacheService RPC with leader failover.

        Retries when the transport is unavailable or the server reports a
        stale-leader error inside the response payload.
        """
        deadline = timeout if timeout is not None else self.call_timeout
        last_exc: Optional[BaseException] = None

        for attempt in range(retries + 1):
            try:
                stub = self._kv_stub()
                wire_name = _RPC_NAMES[rpc_name]
                method = getattr(stub, wire_name)
                response = method(request, timeout=deadline)
            except grpc.RpcError as exc:  # transport-level failure
                last_exc = exc
                code = exc.code() if hasattr(exc, "code") else None
                if code == grpc.StatusCode.UNAVAILABLE:
                    self._safe_refresh()
                    continue
                if code == grpc.StatusCode.DEADLINE_EXCEEDED and attempt < retries:
                    self._safe_refresh()
                    continue
                raise KVError(f"{rpc_name} failed: {exc}", -1) from exc

            # Business payload may embed a stale-leader error.
            err = getattr(response, "error", "")
            if err and isinstance(err, str) and _NOT_LEADER_RE.search(err):
                self._safe_refresh()
                continue

            return response

        raise KVError(
            f"{rpc_name} failed after {retries + 1} attempts: {last_exc}",
            -1,
        )

    def _safe_refresh(self) -> None:
        try:
            self.refresh_leader()
        except KVError:
            pass

    # ==================================================================
    # Session / Block (PagedAttention path)
    # ==================================================================

    def create_session(
        self,
        session_id: str,
        model_name: str,
        num_layers: int,
        num_heads: int,
        head_dim: int,
        dtype: str = "fp16",
        ttl_seconds: int = 0,
        owner_id: str = "",
        namespace_id: str = "",
        collection: str = "",
        timeout: Optional[float] = None,
    ) -> pb.CreateSessionResponse:
        req = pb.CreateSessionRequest(
            session_id=session_id,
            model_name=model_name,
            num_layers=num_layers,
            num_heads=num_heads,
            head_dim=head_dim,
            dtype=dtype,
            ttl_seconds=ttl_seconds,
            owner_id=owner_id,
            namespace_id=namespace_id,
            collection=collection,
        )
        return self.call("create_session", req, timeout=timeout)

    def delete_session(
        self, session_id: str, timeout: Optional[float] = None
    ) -> pb.DeleteSessionResponse:
        return self.call(
            "delete_session",
            pb.DeleteSessionRequest(session_id=session_id),
            timeout=timeout,
        )

    def get_session(
        self, session_id: str, timeout: Optional[float] = None
    ) -> pb.GetSessionResponse:
        return self.call(
            "get_session",
            pb.GetSessionRequest(session_id=session_id),
            timeout=timeout,
        )

    def list_sessions(
        self,
        limit: int = 0,
        prefix: str = "",
        timeout: Optional[float] = None,
    ) -> pb.ListSessionsResponse:
        return self.call(
            "list_sessions",
            pb.ListSessionsRequest(limit=limit, prefix=prefix),
            timeout=timeout,
        )

    def put_block(
        self,
        session_id: str,
        layer_id: int,
        num_tokens: int,
        data: bytes,
        timeout: Optional[float] = None,
    ) -> pb.PutBlockResponse:
        req = pb.PutBlockRequest(
            session_id=session_id,
            layer_id=layer_id,
            num_tokens=num_tokens,
            data=data,
        )
        return self.call("put_block", req, timeout=timeout)

    def get_block(
        self, block_id: int, timeout: Optional[float] = None
    ) -> pb.GetBlockResponse:
        return self.call(
            "get_block",
            pb.GetBlockRequest(block_id=block_id),
            timeout=timeout,
        )

    def batch_put_blocks(
        self,
        blocks: Sequence[pb.PutBlockRequest],
        timeout: Optional[float] = None,
    ) -> pb.BatchPutResponse:
        return self.call(
            "batch_put",
            pb.BatchPutRequest(blocks=list(blocks)),
            timeout=timeout,
        )

    def batch_get_blocks(
        self, block_ids: Sequence[int], timeout: Optional[float] = None
    ) -> pb.BatchGetResponse:
        return self.call(
            "batch_get",
            pb.BatchGetRequest(block_ids=list(block_ids)),
            timeout=timeout,
        )

    # ==================================================================
    # Stats / Namespace
    # ==================================================================

    def get_stats(self, timeout: Optional[float] = None) -> pb.GetStatsResponse:
        return self.call("get_stats", pb.GetStatsRequest(), timeout=timeout)

    def create_namespace(
        self,
        namespace_id: str,
        name: str,
        owner_id: str = "",
        timeout: Optional[float] = None,
    ) -> pb.CreateNamespaceResponse:
        return self.call(
            "create_namespace",
            pb.CreateNamespaceRequest(
                namespace_id=namespace_id, name=name, owner_id=owner_id
            ),
            timeout=timeout,
        )

    def list_namespaces(
        self, owner_id: str = "", timeout: Optional[float] = None
    ) -> pb.ListNamespacesResponse:
        return self.call(
            "list_namespaces",
            pb.ListNamespacesRequest(owner_id=owner_id),
            timeout=timeout,
        )

    def get_namespace(
        self,
        namespace_id: str,
        owner_id: str = "",
        timeout: Optional[float] = None,
    ) -> pb.GetNamespaceResponse:
        return self.call(
            "get_namespace",
            pb.GetNamespaceRequest(
                namespace_id=namespace_id, owner_id=owner_id
            ),
            timeout=timeout,
        )

    def delete_namespace(
        self,
        namespace_id: str,
        owner_id: str = "",
        timeout: Optional[float] = None,
    ) -> pb.DeleteNamespaceResponse:
        return self.call(
            "delete_namespace",
            pb.DeleteNamespaceRequest(
                namespace_id=namespace_id, owner_id=owner_id
            ),
            timeout=timeout,
        )

    # ==================================================================
    # Generic key-value
    # ==================================================================

    def kv_put(
        self,
        namespace_id: str,
        key: str,
        value: bytes,
        owner_id: str = "",
        ttl_seconds: int = 0,
        timeout: Optional[float] = None,
    ) -> pb.KVResponse:
        req = pb.KVPutRequest(
            namespace_id=namespace_id,
            key=key,
            value=value,
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
        )
        return self.call("k_v_put", req, timeout=timeout)

    def kv_get_raw(
        self,
        namespace_id: str,
        key: str,
        timeout: Optional[float] = None,
    ) -> pb.KVGetResponse:
        return self.call(
            "k_v_get",
            pb.KVGetRequest(namespace_id=namespace_id, key=key),
            timeout=timeout,
        )

    def kv_delete(
        self, namespace_id: str, key: str, timeout: Optional[float] = None
    ) -> pb.KVResponse:
        return self.call(
            "k_v_delete",
            pb.KVDeleteRequest(namespace_id=namespace_id, key=key),
            timeout=timeout,
        )

    def kv_exists(
        self,
        namespace_id: str,
        key: str,
        timeout: Optional[float] = None,
    ) -> pb.KVExistsResponse:
        return self.call(
            "k_v_exists",
            pb.KVExistsRequest(namespace_id=namespace_id, key=key),
            timeout=timeout,
        )

    def kv_list(
        self,
        namespace_id: str,
        prefix: str = "",
        timeout: Optional[float] = None,
    ) -> pb.KVListResponse:
        return self.call(
            "k_v_list",
            pb.KVListRequest(namespace_id=namespace_id, prefix=prefix),
            timeout=timeout,
        )

    def kv_remove_by_regex(
        self,
        namespace_id: str,
        pattern: str,
        timeout: Optional[float] = None,
    ) -> pb.KVResponse:
        return self.call(
            "k_v_remove_by_regex",
            pb.KVRemoveByRegexRequest(
                namespace_id=namespace_id, pattern=pattern
            ),
            timeout=timeout,
        )

    def kv_remove_all(
        self, namespace_id: str, timeout: Optional[float] = None
    ) -> pb.KVResponse:
        return self.call(
            "k_v_remove_all",
            pb.KVRemoveAllRequest(namespace_id=namespace_id),
            timeout=timeout,
        )

    def kv_batch_put(
        self,
        namespace_id: str,
        keys: Sequence[str],
        values: Sequence[bytes],
        owner_id: str = "",
        timeout: Optional[float] = None,
    ) -> pb.KVBatchResponse:
        if len(keys) != len(values):
            raise ValueError("keys and values must have equal length")
        req = pb.KVBatchPutRequest(
            namespace_id=namespace_id,
            keys=list(keys),
            values=list(values),
            owner_id=owner_id,
        )
        return self.call("k_v_batch_put", req, timeout=timeout)

    def kv_batch_get(
        self,
        namespace_id: str,
        keys: Sequence[str],
        timeout: Optional[float] = None,
    ) -> pb.KVBatchGetResponse:
        return self.call(
            "k_v_batch_get",
            pb.KVBatchGetRequest(
                namespace_id=namespace_id, keys=list(keys)
            ),
            timeout=timeout,
        )
