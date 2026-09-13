#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
measure_delta.py — follow-up to analyze_chunks.py.

The exact-chunk result was ~0 (see results_report.md). This script asks:
where *is* the redundancy across adjacent checkpoints? It compares adjacent
steps on the torch zip payload stream (data/N tensor storages) and measures:

  - exact_4k_ratio      : fraction of aligned 4K blocks byte-identical
  - byte_equal_ratio    : fraction of individual bytes equal (block similarity)
  - zero_4k_ratio       : fraction of aligned 4K blocks that are all-zero
  - zstd_single_ratio   : compressibility of one checkpoint (zstd -1)
  - zstd_xor_ratio      : compressibility of the per-byte XOR delta stream
                          (large ratio => values change little, i.e. the
                          redundancy is numeric similarity, not equal blocks)

Only selected adjacent pairs are measured (default (0,1),(9,10),(18,19));
torch zip tensor-storage entries are paired 1:1 across steps (same order,
equal sizes — verified). deps: zstandard, numpy.
"""

import argparse
import csv
import json
import os
import zipfile

import numpy as np
import zstandard as zstd

BLOCK = 64 * 1024 * 1024


def payload_entries(path):
    with zipfile.ZipFile(path) as z:
        ents = [i for i in z.infolist()
                if os.path.basename(i.filename).split(".")[0].isdigit()
                and os.path.dirname(i.filename).endswith("data")]
        ents.sort(key=lambda i: int(os.path.basename(i.filename)))
        return [(i.filename, i.file_size) for i in ents]


class ZCounter:
    def __init__(self, level=1):
        self.c = zstd.ZstdCompressor(level=level)
        self.n = 0
        self.buf = []

    def add(self, data):
        self.n += len(data)
        self.buf.append(self.c.compress(data))

    def ratio(self):
        comp = sum(len(x) for x in self.buf)
        return round(self.n / comp, 3) if comp else 0.0


def measure_pair(p_old, p_new):
    ea = payload_entries(p_old)
    eb = payload_entries(p_new)
    # arcnames carry the save-file prefix (step_xxx/data/N); compare by N
    assert [int(os.path.basename(n)) for n, _ in ea] == \
           [int(os.path.basename(n)) for n, _ in eb], "entry layout mismatch"
    za = zipfile.ZipFile(p_old)
    zb = zipfile.ZipFile(p_new)
    total = exact4k = zero4k = eq_bytes = 0
    znew = ZCounter()
    zxor = ZCounter()
    for (name_a, size_a), (name_b, size_b) in zip(ea, eb):
        assert size_a == size_b, f"entry {name_a} size drift {size_a} {size_b}"
        fa, fb = za.open(name_a), zb.open(name_b)
        while True:
            a = fa.read(BLOCK)
            b = fb.read(BLOCK)
            if not a and not b:
                break
            assert len(a) == len(b)
            aa = np.frombuffer(a, dtype=np.uint8)
            bb = np.frombuffer(b, dtype=np.uint8)
            total += len(a)
            eq = aa == bb
            eq_bytes += int(eq.sum())
            nb = (len(a) // 4096) * 4096
            if nb:
                e4 = eq[:nb].reshape(-1, 4096).all(axis=1)
                exact4k += int(e4.sum())
                z = (bb[:nb].reshape(-1, 4096) == 0).all(axis=1)
                zero4k += int(z.sum())
            znew.add(bytes(b))
            zxor.add(bytes((aa ^ bb).tobytes()))
        fa.close(); fb.close()
    za.close(); zb.close()
    return {
        "bytes": total,
        "exact_4k_ratio": round(exact4k / (total // 4096), 6),
        "zero_4k_ratio": round(zero4k / (total // 4096), 6),
        "byte_equal_ratio": round(eq_bytes / total, 6),
        "zstd_single_ratio": znew.ratio(),
        "zstd_xor_ratio": zxor.ratio(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="../../output/checkpoint-dedup")
    ap.add_argument("--out", default="results")
    ap.add_argument("--pairs", default="0:1,9:10,18:19")
    args = ap.parse_args()
    pairs = [tuple(map(int, p.split(":"))) for p in args.pairs.split(",")]

    rows = []
    for fmt, sub in [("weights", "weights"), ("torchsave", "torchsave")]:
        for a, b in pairs:
            pa = os.path.join(args.inp, sub, f"step_{a:03d}.pt")
            pb = os.path.join(args.inp, sub, f"step_{b:03d}.pt")
            r = measure_pair(pa, pb)
            r.update({"format": fmt, "step_a": a, "step_b": b, "view": "zipdata"})
            rows.append(r)
            print(fmt, (a, b), r)

    raw = os.path.join(args.out, "raw")
    os.makedirs(raw, exist_ok=True)
    fields = ["format", "view", "step_a", "step_b", "bytes", "exact_4k_ratio",
              "zero_4k_ratio", "byte_equal_ratio", "zstd_single_ratio",
              "zstd_xor_ratio"]
    with open(os.path.join(raw, "delta_compression.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in fields})
    with open(os.path.join(args.out, "delta_results.json"), "w") as f:
        json.dump(rows, f, indent=2)


if __name__ == "__main__":
    main()
