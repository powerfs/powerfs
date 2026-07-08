import requests
import json
import hmac
import hashlib
import time
import re
import base64
from typing import Optional, Dict, Any, List, Tuple, Union
import numpy as np


class ReplicateConfig:
    def __init__(self):
        self.replica_num = 1
        self.with_soft_pin = False
        self.preferred_segment = ""


class KVClient:
    def __init__(self):
        self.base_url = ""
        self.namespace = "default"
        self.session_id = None
        self.access_key = None
        self.secret_key = None
        self._registered_buffers = set()

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
        try:
            self.base_url = metadata_server.rstrip("/")
            return 0
        except Exception:
            return -1

    def _sign_request(self, method: str, path: str, body: Optional[str] = None) -> Dict[str, str]:
        if not self.access_key or not self.secret_key:
            return {}

        timestamp = str(int(time.time()))
        content = f"{method}\n{path}\n{timestamp}\n{body or ''}"
        signature = hmac.new(
            self.secret_key.encode(),
            content.encode(),
            hashlib.sha256
        ).hexdigest()

        return {
            "X-KV-Access-Key": self.access_key,
            "X-KV-Timestamp": timestamp,
            "X-KV-Signature": signature,
        }

    def _request(self, method: str, path: str, **kwargs) -> Tuple[int, Any]:
        url = f"{self.base_url}{path}"
        headers = kwargs.pop("headers", {})
        headers.update(self._sign_request(method, path, kwargs.get("data")))

        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            if response.status_code == 200:
                data = response.json()
                if data.get("success", True):
                    return 0, data.get("data")
                else:
                    error_msg = data.get("error", "Unknown error")
                    if "not found" in error_msg.lower():
                        return -2, error_msg
                    elif "permission" in error_msg.lower():
                        return -3, error_msg
                    return -1, error_msg
            elif response.status_code == 404:
                return -2, "Not found"
            elif response.status_code == 403:
                return -3, "Permission denied"
            return -1, f"HTTP error {response.status_code}"
        except requests.exceptions.RequestException as e:
            return -1, str(e)

    def put(self, key: str, value: bytes, config: Optional[ReplicateConfig] = None) -> int:
        path = f"/kv/put/{key}"
        params = {"namespace": self.namespace}
        if config and config.replica_num > 1:
            params["replica_num"] = config.replica_num
        if config and config.with_soft_pin:
            params["soft_pin"] = "true"

        code, _ = self._request("PUT", path, params=params, data=value)
        return code

    def get(self, key: str) -> Tuple[int, Optional[bytes]]:
        path = f"/kv/get/{key}"
        params = {"namespace": self.namespace}

        code, data = self._request("GET", path, params=params)
        if code == 0 and data is not None:
            if isinstance(data, str):
                return 0, base64.b64decode(data)
            elif isinstance(data, bytes):
                return 0, data
        return code, None

    def put_batch(self, keys: List[str], values: List[bytes]) -> List[int]:
        path = "/kv/put_batch"
        params = {"namespace": self.namespace}
        encoded_values = [base64.b64encode(v).decode() for v in values]

        code, results = self._request("POST", path, params=params, json={"keys": keys, "values": encoded_values})
        if code == 0 and results:
            return [0 if r else -1 for r in results]
        return [-1] * len(keys)

    def get_batch(self, keys: List[str]) -> Tuple[int, List[Optional[bytes]]]:
        path = "/kv/get_batch"
        params = {"namespace": self.namespace}

        code, results = self._request("POST", path, params=params, json={"keys": keys})
        if code == 0 and results:
            decoded = []
            for item in results:
                if item is None:
                    decoded.append(None)
                elif isinstance(item, str):
                    decoded.append(base64.b64decode(item))
                else:
                    decoded.append(None)
            return 0, decoded
        return code, [None] * len(keys)

    def is_exist(self, key: str) -> int:
        path = f"/kv/exists/{key}"
        params = {"namespace": self.namespace}

        code, data = self._request("GET", path, params=params)
        if code == 0:
            if data is True or data == 1:
                return 1
            elif data is False or data == 0:
                return 0
        return -1

    def remove(self, key: str) -> int:
        path = f"/kv/delete/{key}"
        params = {"namespace": self.namespace}

        code, _ = self._request("DELETE", path, params=params)
        return code

    def remove_by_regex(self, pattern: str) -> int:
        path = "/kv/remove_by_regex"
        params = {"namespace": self.namespace, "pattern": pattern}

        code, _ = self._request("DELETE", path, params=params)
        return code

    def remove_all(self) -> int:
        path = "/kv/remove_all"
        params = {"namespace": self.namespace}

        code, _ = self._request("DELETE", path, params=params)
        return code

    def register_buffer(self, ptr: int, size: int) -> int:
        self._registered_buffers.add(ptr)
        return 0

    def unregister_buffer(self, ptr: int) -> int:
        self._registered_buffers.discard(ptr)
        return 0

    def put_from(self, key: str, ptr: int, size: int) -> int:
        if ptr not in self._registered_buffers:
            return -1

        try:
            buf = np.frombuffer(np.ctypeslib.as_ctypes(np.empty(size, dtype=np.uint8)), dtype=np.uint8)
            buf_ptr = buf.ctypes.data
            if buf_ptr != ptr:
                return -1

            data = buf.tobytes()
            return self.put(key, data)
        except Exception:
            return -1

    def get_into(self, key: str, ptr: int, size: int) -> Tuple[int, int]:
        if ptr not in self._registered_buffers:
            return -1, 0

        code, data = self.get(key)
        if code != 0 or data is None:
            return code, 0

        try:
            buf = np.frombuffer(np.ctypeslib.as_ctypes(np.empty(size, dtype=np.uint8)), dtype=np.uint8)
            buf_ptr = buf.ctypes.data
            if buf_ptr != ptr:
                return -1, 0

            copy_len = min(size, len(data))
            buf[:copy_len] = np.frombuffer(data[:copy_len], dtype=np.uint8)
            return 0, copy_len
        except Exception:
            return -1, 0

    def batch_put_from(self, keys: List[str], ptrs: List[int], sizes: List[int]) -> List[int]:
        results = []
        for key, ptr, size in zip(keys, ptrs, sizes):
            results.append(self.put_from(key, ptr, size))
        return results

    def batch_get_into(self, keys: List[str], ptrs: List[int], sizes: List[int]) -> List[int]:
        results = []
        for key, ptr, size in zip(keys, ptrs, sizes):
            code, _ = self.get_into(key, ptr, size)
            results.append(code)
        return results

    def put_tensor(self, key: str, tensor: Any, config: Optional[ReplicateConfig] = None) -> int:
        try:
            import torch
            if isinstance(tensor, torch.Tensor):
                data = tensor.cpu().numpy().tobytes()
                return self.put(key, data)
            else:
                return -1
        except ImportError:
            return -2

    def get_tensor(self, key: str) -> Tuple[int, Optional[Any]]:
        try:
            import torch
            code, data = self.get(key)
            if code != 0 or data is None:
                return code, None
            
            return 0, torch.from_numpy(np.frombuffer(data))
        except ImportError:
            return -2, None

    def batch_put_tensor(self, keys: List[str], tensors: List[Any]) -> List[int]:
        results = []
        for key, tensor in zip(keys, tensors):
            results.append(self.put_tensor(key, tensor))
        return results

    def batch_get_tensor(self, keys: List[str]) -> List[Optional[Any]]:
        results = []
        for key in keys:
            code, tensor = self.get_tensor(key)
            results.append(tensor if code == 0 else None)
        return results

    def put_tensor_with_tp(self, key: str, tensor: Any, tp_rank: int, tp_size: int, split_dim: int) -> int:
        try:
            import torch
            if not isinstance(tensor, torch.Tensor):
                return -1

            if tp_size == 1:
                return self.put_tensor(key, tensor)

            dim_size = tensor.shape[split_dim] // tp_size
            start = tp_rank * dim_size
            end = start + dim_size
            if tp_rank == tp_size - 1:
                end = tensor.shape[split_dim]

            sliced = torch.narrow(tensor, split_dim, start, end - start)
            shard_key = f"{key}_tp{tp_rank}"
            return self.put_tensor(shard_key, sliced)
        except ImportError:
            return -2

    def get_tensor_with_tp(self, key: str, tp_rank: int, tp_size: int) -> Tuple[int, Optional[Any]]:
        try:
            import torch
            shard_key = f"{key}_tp{tp_rank}"
            code, shard = self.get_tensor(shard_key)
            if code != 0 or shard is None:
                return code, None
            return 0, shard
        except ImportError:
            return -2, None

    def list_keys(self, prefix: Optional[str] = None) -> Tuple[int, List[str]]:
        path = "/kv/list"
        params = {"namespace": self.namespace}
        if prefix:
            params["prefix"] = prefix

        code, data = self._request("GET", path, params=params)
        if code == 0 and data:
            return 0, data
        return code, []

    def close(self) -> int:
        self._registered_buffers.clear()
        return 0


class KVAdminClient:
    def __init__(self):
        self.base_url = ""
        self.token = None

    def setup(self, base_url: str, token: Optional[str] = None) -> int:
        self.base_url = base_url.rstrip("/")
        self.token = token
        return 0

    def _request(self, method: str, path: str, **kwargs) -> Tuple[int, Any]:
        url = f"{self.base_url}{path}"
        headers = kwargs.pop("headers", {})
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            data = response.json()
            if data.get("success", True):
                return 0, data.get("data")
            else:
                return -1, data.get("error", "Unknown error")
        except requests.exceptions.RequestException as e:
            return -1, str(e)

    def create_namespace(self, name: str) -> Tuple[int, Dict[str, Any]]:
        path = "/api/kv/namespaces"
        code, data = self._request("POST", path, json={"name": name})
        return code, data if code == 0 else {}

    def list_namespaces(self) -> Tuple[int, List[Dict[str, Any]]]:
        path = "/api/kv/namespaces"
        code, data = self._request("GET", path)
        return code, data if code == 0 else []

    def get_namespace(self, namespace_id: str) -> Tuple[int, Dict[str, Any]]:
        path = f"/api/kv/namespaces/{namespace_id}"
        code, data = self._request("GET", path)
        return code, data if code == 0 else {}

    def delete_namespace(self, namespace_id: str) -> int:
        path = f"/api/kv/namespaces/{namespace_id}"
        code, _ = self._request("DELETE", path)
        return code

    def create_api_key(self) -> Tuple[int, Dict[str, Any]]:
        path = "/api/kv/keys"
        code, data = self._request("POST", path)
        return code, data if code == 0 else {}

    def list_api_keys(self) -> Tuple[int, List[Dict[str, Any]]]:
        path = "/api/kv/keys"
        code, data = self._request("GET", path)
        return code, data if code == 0 else []

    def delete_api_key(self, key_id: str) -> int:
        path = f"/api/kv/keys/{key_id}"
        code, _ = self._request("DELETE", path)
        return code

    def get_metrics(self) -> Tuple[int, Dict[str, Any]]:
        path = "/api/metrics/kv"
        code, data = self._request("GET", path)
        return code, data if code == 0 else {}

    def get_sessions(self) -> Tuple[int, List[Dict[str, Any]]]:
        path = "/api/metrics/kv/sessions"
        code, data = self._request("GET", path)
        return code, data if code == 0 else []
