#!/usr/bin/env python3
"""PowerFS KV Benchmark (Mooncake compatible)"""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
import statistics
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from powerfs.kv import KVClient, ReplicateConfig


LOG = logging.getLogger("kv_bench")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PowerFS KV Benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=["verify_write", "fill", "write_perf", "read_perf", "mixed_rw"],
        help="Benchmark scenario to execute.",
    )

    parser.add_argument("--metadata-server", default="http://127.0.0.1:8080")
    parser.add_argument("--numjobs", type=int, default=1)
    parser.add_argument("--iodepth", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--runtime", type=int, default=0, help="Seconds. 0 means object-count based.")
    parser.add_argument("--nr-objects", type=int, default=128)
    parser.add_argument("--write-objects", type=int, default=0)
    parser.add_argument(
        "--prepare-objects",
        type=int,
        default=0,
        help="Object count used by the prepare phase. 0 means reuse nr-objects.",
    )
    parser.add_argument("--object-id-start", type=int, default=0)
    parser.add_argument("--key-prefix", default="kvbench")
    parser.add_argument("--key-size", type=int, default=20)
    parser.add_argument("--value-size", type=int, default=4096)
    parser.add_argument("--rand-seed", type=int, default=1)

    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--pattern", default="")
    parser.add_argument("--prepare-mode", choices=["auto", "none", "write"], default="auto")
    parser.add_argument("--rwmixread", type=int, default=70)

    parser.add_argument("--log-level", default="INFO")
    return parser


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@dataclass
class PhaseStats:
    name: str
    request_latencies: List[float] = field(default_factory=list)
    requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    kvs: int = 0
    successful_kvs: int = 0
    failed_kvs: int = 0
    misses: int = 0
    verify_failures: int = 0
    bytes_processed: int = 0
    error_counts: Counter = field(default_factory=Counter)
    start_time: float = 0.0
    end_time: float = 0.0
    dataset_exhausted: bool = False


@dataclass
class RequestResult:
    request_ok: bool
    kv_successes: int
    kv_failures: int
    bytes_processed: int
    successful_object_ids: List[int] = field(default_factory=list)
    misses: int = 0
    verify_failures: int = 0
    error_counts: Counter = field(default_factory=Counter)


class PayloadFactory:
    def __init__(self, value_size: int, pattern: bytes):
        self.value_size = value_size
        self.pattern = pattern
        self._default_cache: dict[int, bytes] = {}
        self._pattern_payload = self._repeat(pattern) if pattern else b""

    def _repeat(self, token: bytes) -> bytes:
        repeat = (self.value_size + len(token) - 1) // len(token)
        return (token * repeat)[: self.value_size]

    def build(self, object_id: int) -> bytes:
        if self.pattern:
            return self._pattern_payload
        fill_byte = object_id & 0xFF
        payload = self._default_cache.get(fill_byte)
        if payload is None:
            payload = bytes([fill_byte]) * self.value_size
            self._default_cache[fill_byte] = payload
        return payload

    def verify_payload(self, object_id: int, payload: bytes) -> bool:
        return payload == self.build(object_id)


class DatasetState:
    def __init__(self, object_id_start: int):
        self.write_lock = threading.Lock()
        self.ids_lock = threading.Lock()
        self.cursor_lock = threading.Lock()
        self.next_write_id = object_id_start
        self.prepared_ids: tuple[int, ...] = ()
        self.written_ids: tuple[int, ...] = ()
        self.read_cursor = 0

    def reserve_write_ids(self, count: int, upper_bound: int) -> List[int]:
        with self.write_lock:
            if self.next_write_id >= upper_bound:
                return []
            end = min(self.next_write_id + count, upper_bound)
            ids = list(range(self.next_write_id, end))
            self.next_write_id = end
            return ids

    def mark_prepared(self, ids: Iterable[int]) -> None:
        ids = tuple(ids)
        if not ids:
            return
        with self.ids_lock:
            self.prepared_ids = self.prepared_ids + ids
            self.written_ids = self.written_ids + ids

    def mark_runtime_written(self, ids: Iterable[int]) -> None:
        ids = tuple(ids)
        if not ids:
            return
        with self.ids_lock:
            self.written_ids = self.written_ids + ids

    def written_count(self) -> int:
        with self.ids_lock:
            return len(self.written_ids)

    def snapshot_written_ids(self) -> List[int]:
        with self.ids_lock:
            return list(self.written_ids)

    def next_read_ids(
        self,
        count: int,
        *,
        loop: bool,
        sequential: bool,
        rng,
        source: str = "written",
    ) -> List[int]:
        with self.ids_lock:
            readable_ids = self.prepared_ids if source == "prepared" else self.written_ids
        if not readable_ids:
            return []
        if sequential:
            with self.cursor_lock:
                result = []
                for _ in range(count):
                    if self.read_cursor >= len(readable_ids):
                        if not loop:
                            break
                        self.read_cursor = 0
                    result.append(readable_ids[self.read_cursor])
                    self.read_cursor += 1
                return result
        return [readable_ids[rng.randrange(len(readable_ids))] for _ in range(count)]


def parse_pattern(pattern_text: str) -> bytes:
    if not pattern_text:
        return b""
    if pattern_text.startswith("0x"):
        hex_text = pattern_text[2:]
        if len(hex_text) % 2 != 0:
            raise ValueError("hex pattern length must be even")
        return bytes.fromhex(hex_text)
    return pattern_text.encode("utf-8")


def make_key(prefix: str, key_size: int, object_id: int) -> str:
    suffix = f"{object_id:016d}"
    if key_size < len(suffix):
        raise ValueError(f"key_size={key_size} is smaller than suffix length {len(suffix)}")
    prefix_space = key_size - len(suffix)
    prefix_part = prefix[:prefix_space].ljust(prefix_space, "_")
    return f"{prefix_part}{suffix}"


class StoreSession:
    def __init__(
        self,
        args: argparse.Namespace,
        lane_id: int,
        payload_factory: PayloadFactory,
        client: KVClient,
    ):
        self.args = args
        self.lane_id = lane_id
        self.payload_factory = payload_factory
        self.client = client

    def put_ids(self, object_ids: List[int]) -> RequestResult:
        keys = [make_key(self.args.key_prefix, self.args.key_size, object_id) for object_id in object_ids]
        values = [self.payload_factory.build(object_id) for object_id in object_ids]

        errors: Counter = Counter()
        success_ids: List[int] = []

        if len(object_ids) == 1 and self.args.batch_size == 1:
            ret = self.client.put(keys[0], values[0])
            if ret == 0:
                success_ids.extend(object_ids)
            else:
                errors[ret] += 1
        else:
            ret_codes = self.client.put_batch(keys, values)
            for object_id, ret in zip(object_ids, ret_codes):
                if ret == 0:
                    success_ids.append(object_id)
                else:
                    errors[ret] += 1

        request_ok = len(success_ids) == len(object_ids)
        return RequestResult(
            request_ok=request_ok,
            kv_successes=len(success_ids),
            kv_failures=len(object_ids) - len(success_ids),
            bytes_processed=len(success_ids) * self.args.value_size,
            successful_object_ids=success_ids,
            error_counts=errors,
        )

    def get_ids(self, object_ids: List[int], verify: bool) -> RequestResult:
        keys = [make_key(self.args.key_prefix, self.args.key_size, object_id) for object_id in object_ids]
        errors: Counter = Counter()
        kv_successes = 0
        misses = 0
        verify_failures = 0

        if len(object_ids) == 1 and self.args.batch_size == 1:
            code, payload = self.client.get(keys[0])
            if code != 0 or payload is None:
                misses += 1
                errors["MISS"] += 1
            else:
                if verify and not self.payload_factory.verify_payload(object_ids[0], payload):
                    verify_failures += 1
                    errors["VERIFY_FAIL"] += 1
                else:
                    kv_successes += 1
        else:
            code, payloads = self.client.get_batch(keys)
            if code != 0:
                misses += len(object_ids)
                errors["BATCH_ERROR"] += 1
            else:
                for object_id, payload in zip(object_ids, payloads):
                    if payload is None:
                        misses += 1
                        errors["MISS"] += 1
                    else:
                        if verify and not self.payload_factory.verify_payload(object_id, payload):
                            verify_failures += 1
                            errors["VERIFY_FAIL"] += 1
                        else:
                            kv_successes += 1

        kv_failures = len(object_ids) - kv_successes
        return RequestResult(
            request_ok=(kv_failures == 0),
            kv_successes=kv_successes,
            kv_failures=kv_failures,
            bytes_processed=kv_successes * self.args.value_size,
            misses=misses,
            verify_failures=verify_failures,
            error_counts=errors,
        )


class StoreRuntime:
    def __init__(self, args: argparse.Namespace):
        self.client = KVClient()
        setup_ret = self.client.setup(
            "localhost",
            args.metadata_server,
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            "tcp",
            "",
            "",
        )
        if setup_ret != 0:
            raise RuntimeError(f"setup failed: {setup_ret}")

    def make_session(
        self,
        args: argparse.Namespace,
        lane_id: int,
        payload_factory: PayloadFactory,
    ) -> StoreSession:
        return StoreSession(args, lane_id, payload_factory, self.client)

    def close(self) -> None:
        self.client.close()


def merge_stats(name: str, stats_list: List[PhaseStats]) -> PhaseStats:
    merged = PhaseStats(name=name)
    if not stats_list:
        return merged
    merged.start_time = min((s.start_time for s in stats_list if s.start_time), default=0.0)
    merged.end_time = max((s.end_time for s in stats_list if s.end_time), default=0.0)
    for stats in stats_list:
        merged.request_latencies.extend(stats.request_latencies)
        merged.requests += stats.requests
        merged.successful_requests += stats.successful_requests
        merged.failed_requests += stats.failed_requests
        merged.kvs += stats.kvs
        merged.successful_kvs += stats.successful_kvs
        merged.failed_kvs += stats.failed_kvs
        merged.misses += stats.misses
        merged.verify_failures += stats.verify_failures
        merged.bytes_processed += stats.bytes_processed
        merged.error_counts.update(stats.error_counts)
        merged.dataset_exhausted = merged.dataset_exhausted or stats.dataset_exhausted
    return merged


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * p
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def summarize_stats(stats: PhaseStats) -> dict:
    duration = max(stats.end_time - stats.start_time, 0.0)
    return {
        "requests": stats.requests,
        "successful_requests": stats.successful_requests,
        "failed_requests": stats.failed_requests,
        "kvs": stats.kvs,
        "successful_kvs": stats.successful_kvs,
        "failed_kvs": stats.failed_kvs,
        "misses": stats.misses,
        "verify_failures": stats.verify_failures,
        "bytes": stats.bytes_processed,
        "duration_sec": duration,
        "req_per_sec": (stats.requests / duration) if duration > 0 else 0.0,
        "kv_per_sec": (stats.kvs / duration) if duration > 0 else 0.0,
        "MiB_per_sec": (stats.bytes_processed / duration / (1024 * 1024)) if duration > 0 else 0.0,
        "lat_mean_ms": statistics.mean(stats.request_latencies) * 1000 if stats.request_latencies else 0.0,
        "lat_p50_ms": percentile(stats.request_latencies, 0.50) * 1000,
        "lat_p95_ms": percentile(stats.request_latencies, 0.95) * 1000,
        "lat_p99_ms": percentile(stats.request_latencies, 0.99) * 1000,
        "dataset_exhausted": stats.dataset_exhausted,
        "error_counts": dict(stats.error_counts),
    }


def log_phase_stats(stats: PhaseStats) -> None:
    summary = summarize_stats(stats)
    LOG.info("=== phase %s ===", stats.name)
    LOG.info(
        "requests=%d successful_requests=%d failed_requests=%d kvs=%d successful_kvs=%d failed_kvs=%d",
        summary["requests"],
        summary["successful_requests"],
        summary["failed_requests"],
        summary["kvs"],
        summary["successful_kvs"],
        summary["failed_kvs"],
    )
    LOG.info(
        "misses=%d verify_failures=%d bytes=%d duration=%.3fs req/s=%.2f kv/s=%.2f MiB/s=%.2f",
        summary["misses"],
        summary["verify_failures"],
        summary["bytes"],
        summary["duration_sec"],
        summary["req_per_sec"],
        summary["kv_per_sec"],
        summary["MiB_per_sec"],
    )
    LOG.info(
        "lat_mean=%.3fms lat_p50=%.3fms lat_p95=%.3fms lat_p99=%.3fms dataset_exhausted=%s",
        summary["lat_mean_ms"],
        summary["lat_p50_ms"],
        summary["lat_p95_ms"],
        summary["lat_p99_ms"],
        summary["dataset_exhausted"],
    )
    if summary["error_counts"]:
        LOG.info("errors=%s", summary["error_counts"])


class BenchmarkRunner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.pattern = parse_pattern(args.pattern)
        self.payload_factory = PayloadFactory(args.value_size, self.pattern)
        self.dataset = DatasetState(args.object_id_start)
        self.lane_count = args.numjobs * args.iodepth
        self._sessions: Optional[List[StoreSession]] = None
        self._runtime: Optional[StoreRuntime] = None
        self._validate_args()

    def _validate_args(self) -> None:
        if self.args.numjobs <= 0 or self.args.iodepth <= 0:
            raise ValueError("numjobs and iodepth must be > 0")
        if self.args.batch_size <= 0:
            raise ValueError("batch-size must be > 0")
        if self.args.value_size <= 0:
            raise ValueError("value-size must be > 0")
        if self.args.key_size <= 0:
            raise ValueError("key-size must be > 0")
        if self.args.nr_objects <= 0:
            raise ValueError("nr-objects must be > 0")
        if self.args.write_objects < 0:
            raise ValueError("write-objects must be >= 0")
        if self.args.prepare_objects < 0:
            raise ValueError("prepare-objects must be >= 0")
        if self.args.rwmixread < 0 or self.args.rwmixread > 100:
            raise ValueError("rwmixread must be within [0, 100]")
        if self.args.scenario == "mixed_rw" and self.args.runtime <= 0:
            raise ValueError("mixed_rw requires --runtime > 0")
        make_key(self.args.key_prefix, self.args.key_size, self.args.object_id_start)

    def _scenario_has_write(self) -> bool:
        return self.args.scenario in {"verify_write", "fill", "write_perf", "mixed_rw"}

    def _write_budget(self) -> int:
        return self.args.write_objects if self.args.write_objects > 0 else self.args.nr_objects

    def _prepare_budget(self) -> int:
        return self.args.prepare_objects if self.args.prepare_objects > 0 else self.args.nr_objects

    def _make_sessions(self) -> List[StoreSession]:
        if self._sessions is None:
            self._runtime = StoreRuntime(self.args)
            self._sessions = [
                self._runtime.make_session(self.args, lane_id, self.payload_factory)
                for lane_id in range(self.lane_count)
            ]
        return self._sessions

    def close(self) -> None:
        if self._sessions is not None:
            self._sessions = None
        if self._runtime is not None:
            self._runtime.close()
            self._runtime = None

    def _run_threads(self, phase_name: str, worker_builder: Callable[[StoreSession, int], Callable[[PhaseStats], None]]) -> PhaseStats:
        sessions = self._make_sessions()
        per_lane_stats: List[Optional[PhaseStats]] = [None] * self.lane_count
        threads: List[threading.Thread] = []

        def runner(index: int, session: StoreSession) -> None:
            stats = PhaseStats(name=f"{phase_name}/lane{index}")
            stats.start_time = time.perf_counter()
            worker_builder(session, index)(stats)
            stats.end_time = time.perf_counter()
            per_lane_stats[index] = stats

        for lane_id, session in enumerate(sessions):
            thread = threading.Thread(target=runner, args=(lane_id, session), name=f"{phase_name}-lane{lane_id}")
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        merged = merge_stats(phase_name, [s for s in per_lane_stats if s is not None])
        log_phase_stats(merged)
        return merged

    def _record(self, stats: PhaseStats, latency: float, request: RequestResult, kv_count: int) -> None:
        stats.request_latencies.append(latency)
        stats.requests += 1
        stats.kvs += kv_count
        if request.request_ok:
            stats.successful_requests += 1
        else:
            stats.failed_requests += 1
        stats.successful_kvs += request.kv_successes
        stats.failed_kvs += request.kv_failures
        stats.misses += request.misses
        stats.verify_failures += request.verify_failures
        stats.bytes_processed += request.bytes_processed
        stats.error_counts.update(request.error_counts)

    def _run_fixed_write(
        self,
        phase_name: str,
        total_objects: int,
        *,
        strict: bool,
        write_scope: str = "runtime",
    ) -> PhaseStats:
        write_upper = self.dataset.next_write_id + total_objects

        def worker(session: StoreSession, _lane_id: int) -> Callable[[PhaseStats], None]:
            def run(stats: PhaseStats) -> None:
                while True:
                    object_ids = self.dataset.reserve_write_ids(self.args.batch_size, write_upper)
                    if not object_ids:
                        break
                    start = time.perf_counter()
                    result = session.put_ids(object_ids)
                    latency = time.perf_counter() - start
                    self._record(stats, latency, result, len(object_ids))
                    if result.successful_object_ids:
                        if write_scope == "prepared":
                            self.dataset.mark_prepared(result.successful_object_ids)
                        else:
                            self.dataset.mark_runtime_written(result.successful_object_ids)
            return run

        stats = self._run_threads(phase_name, worker)
        expected = total_objects
        if stats.successful_kvs < expected:
            stats.dataset_exhausted = True
        if strict and (stats.failed_kvs > 0 or stats.successful_kvs != expected):
            raise RuntimeError(
                f"{phase_name} strict write failed: expected {expected} objects, "
                f"got success={stats.successful_kvs}, failed={stats.failed_kvs}"
            )
        return stats

    def _run_time_based_write(self, phase_name: str, total_objects: int) -> PhaseStats:
        deadline = time.time() + self.args.runtime
        write_upper = self.dataset.next_write_id + total_objects
        stop_event = threading.Event()

        def worker(session: StoreSession, _lane_id: int) -> Callable[[PhaseStats], None]:
            def run(stats: PhaseStats) -> None:
                while time.time() < deadline and not stop_event.is_set():
                    object_ids = self.dataset.reserve_write_ids(self.args.batch_size, write_upper)
                    if not object_ids:
                        stats.dataset_exhausted = True
                        stop_event.set()
                        break
                    start = time.perf_counter()
                    result = session.put_ids(object_ids)
                    latency = time.perf_counter() - start
                    self._record(stats, latency, result, len(object_ids))
                    if result.successful_object_ids:
                        self.dataset.mark_runtime_written(result.successful_object_ids)
            return run

        return self._run_threads(phase_name, worker)

    def _run_read_phase(
        self,
        phase_name: str,
        *,
        verify: bool,
        sequential: bool,
        loop: bool,
        runtime_sec: int = 0,
    ) -> PhaseStats:
        seed_base = self.args.rand_seed
        if runtime_sec > 0:
            deadline = time.time() + runtime_sec

            def worker(session: StoreSession, lane_id: int) -> Callable[[PhaseStats], None]:
                rng = random.Random(seed_base + lane_id)

                def run(stats: PhaseStats) -> None:
                    while time.time() < deadline:
                        object_ids = self.dataset.next_read_ids(
                            self.args.batch_size,
                            loop=True,
                            sequential=sequential,
                            rng=rng,
                            source="prepared",
                        )
                        if not object_ids:
                            stats.dataset_exhausted = True
                            break
                        start = time.perf_counter()
                        result = session.get_ids(object_ids, verify)
                        latency = time.perf_counter() - start
                        self._record(stats, latency, result, len(object_ids))

                return run

            return self._run_threads(phase_name, worker)

        def worker(session: StoreSession, lane_id: int) -> Callable[[PhaseStats], None]:
            rng = random.Random(seed_base + lane_id)

            def run(stats: PhaseStats) -> None:
                while True:
                    object_ids = self.dataset.next_read_ids(
                        self.args.batch_size,
                        loop=loop,
                        sequential=sequential,
                        rng=rng,
                        source="prepared",
                    )
                    if not object_ids:
                        break
                    start = time.perf_counter()
                    result = session.get_ids(object_ids, verify)
                    latency = time.perf_counter() - start
                    self._record(stats, latency, result, len(object_ids))

            return run

        return self._run_threads(phase_name, worker)

    def _run_mixed_phase(self, phase_name: str, extra_write_budget: int) -> PhaseStats:
        deadline = time.time() + self.args.runtime
        write_upper = self.dataset.next_write_id + extra_write_budget
        stop_event = threading.Event()
        seed_base = self.args.rand_seed

        def worker(session: StoreSession, lane_id: int) -> Callable[[PhaseStats], None]:
            rng = random.Random(seed_base + lane_id)

            def run(stats: PhaseStats) -> None:
                while time.time() < deadline and not stop_event.is_set():
                    do_read = rng.randrange(100) < self.args.rwmixread
                    if do_read:
                        object_ids = self.dataset.next_read_ids(
                            self.args.batch_size,
                            loop=True,
                            sequential=False,
                            rng=rng,
                            source="prepared",
                        )
                        if not object_ids:
                            continue
                        start = time.perf_counter()
                        result = session.get_ids(object_ids, verify=self.args.verify)
                        latency = time.perf_counter() - start
                        self._record(stats, latency, result, len(object_ids))
                        continue

                    object_ids = self.dataset.reserve_write_ids(self.args.batch_size, write_upper)
                    if not object_ids:
                        stats.dataset_exhausted = True
                        stop_event.set()
                        break
                    start = time.perf_counter()
                    result = session.put_ids(object_ids)
                    latency = time.perf_counter() - start
                    self._record(stats, latency, result, len(object_ids))
                    if result.successful_object_ids:
                        self.dataset.mark_runtime_written(result.successful_object_ids)

            return run

        return self._run_threads(phase_name, worker)

    def _maybe_prepare_dataset(self) -> Optional[PhaseStats]:
        if self.args.prepare_mode == "none":
            return None
        if self.args.prepare_mode == "write" or self.args.scenario in {"read_perf", "mixed_rw"}:
            stats = self._run_fixed_write(
                "prepare_write",
                self._prepare_budget(),
                strict=True,
                write_scope="prepared",
            )
            return stats
        return None

    def run(self) -> List[PhaseStats]:
        LOG.info(
            "scenario=%s numjobs=%d iodepth=%d lanes=%d batch_size=%d value_size=%d nr_objects=%d prepare_objects=%d write_objects=%d verify=%s",
            self.args.scenario,
            self.args.numjobs,
            self.args.iodepth,
            self.lane_count,
            self.args.batch_size,
            self.args.value_size,
            self.args.nr_objects,
            self._prepare_budget(),
            self.args.write_objects,
            self.args.verify,
        )

        phases: List[PhaseStats] = []
        if self.args.scenario == "verify_write":
            phases.append(
                self._run_fixed_write(
                    "write_verify",
                    self._write_budget(),
                    strict=True,
                    write_scope="prepared",
                )
            )
            phases.append(self._run_read_phase("verify_read", verify=True, sequential=True, loop=False))
            return phases

        if self.args.scenario == "fill":
            phases.append(self._run_fixed_write("fill_write", self._write_budget(), strict=False))
            return phases

        if self.args.scenario == "write_perf":
            total_objects = self._write_budget()
            if self.args.runtime > 0:
                phases.append(self._run_time_based_write("write_perf", total_objects))
            else:
                phases.append(self._run_fixed_write("write_perf", total_objects, strict=False))
            return phases

        if self.args.scenario == "read_perf":
            prepared = self._maybe_prepare_dataset()
            if prepared is not None:
                phases.append(prepared)
            phases.append(
                self._run_read_phase(
                    "read_perf",
                    verify=self.args.verify,
                    sequential=True,
                    loop=(self.args.runtime > 0),
                    runtime_sec=self.args.runtime,
                )
            )
            return phases

        prepared = self._maybe_prepare_dataset()
        if prepared is not None:
            phases.append(prepared)
        phases.append(self._run_mixed_phase("mixed_rw", self._write_budget()))
        return phases


def log_overall_summary(phases: List[PhaseStats]) -> None:
    overall = merge_stats("overall", phases)
    LOG.info("=== overall summary ===")
    log_phase_stats(overall)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.log_level)
    runner: Optional[BenchmarkRunner] = None
    try:
        runner = BenchmarkRunner(args)
        phases = runner.run()
        log_overall_summary(phases)
        if any(phase.verify_failures > 0 for phase in phases):
            return 20
        if args.verify and any(phase.misses > 0 for phase in phases if "read" in phase.name):
            return 21
        return 0
    except KeyboardInterrupt:
        LOG.warning("benchmark interrupted")
        return 130
    except Exception as exc:
        LOG.exception("benchmark failed: %s", exc)
        return 1
    finally:
        if runner is not None:
            runner.close()


if __name__ == "__main__":
    raise SystemExit(main())