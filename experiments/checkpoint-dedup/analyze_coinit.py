#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
analyze_coinit.py — cross-job overlap for the shared-init fleet fixture.

Compares job B (gen_coinit.py: same init as job A, different data stream)
against job A's weights-only checkpoints at fixed block sizes:

  one-step divergence : A.step_000 blocks found in the shared init snapshot
                        (how much of a byte region survives one AdamW step)
  cross-job same-step : B.step_k vs A.step_k (both k steps from the same
                        init, different data)
  cross-job hit-any   : B.step_k vs union(A.step_0..k)

Writes results/raw/coinit_overlap.csv.
"""

import csv
import os

from analyze_chunks import scan_file

RAW = os.path.join(os.path.dirname(__file__), "results", "raw")
A_W = "../../output/checkpoint-dedup/weights"
B_W = "../../output/checkpoint-dedup/coinit/weights"
SIZES = [4096, 65536, 1048576]


def main():
    a = {s: scan_file(os.path.join(A_W, f"step_{s:03d}.pt"), SIZES)
         for s in range(6)}
    b = {s: scan_file(os.path.join(B_W, f"step_{s:03d}.pt"), SIZES)
         for s in range(6)}
    init = scan_file(os.path.join(B_W, "init.pt"), SIZES)

    out_rows = []

    def emit(kind, step, size, later_fps, earlier_sets):
        blocks = later_fps[size]
        pool = set()
        for s in earlier_sets:
            pool.update(s[size])
        hit = sum(1 for f in blocks if f in pool)
        out_rows.append({"kind": kind, "step": step, "chunk_size": size,
                         "n_blocks": len(blocks), "n_hit": hit,
                         "hit_ratio": hit / len(blocks) if blocks else 0.0})

    for size in SIZES:
        emit("one-step-divergence", 0, size, a[0], [init])
        for k in range(6):
            emit("cross-job-same-step", k, size, b[k], [a[k]])
        for k in range(6):
            emit("cross-job-hit-any", k, size, b[k], [a[j] for j in range(k + 1)])

    os.makedirs(RAW, exist_ok=True)
    path = os.path.join(RAW, "coinit_overlap.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["kind", "step", "chunk_size",
                                          "n_blocks", "n_hit", "hit_ratio"])
        w.writeheader()
        w.writerows(out_rows)
    print("wrote", path)
    for r in out_rows:
        if r["chunk_size"] in (4096, 1048576) and (
                r["kind"] != "cross-job-same-step" or r["step"] <= 1):
            print(f"{r['kind']:22s} step={r['step']} size={r['chunk_size']:>8d} "
                  f"hit={r['hit_ratio']:.6f}")


if __name__ == "__main__":
    main()
