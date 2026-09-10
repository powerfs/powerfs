#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
analyze_controls.py — exact-chunk overlap for the gen_controls.py fixture.

Reports zipdata-view identical-chunk ratio for fp32 vs bf16 weights across
adjacent and long (50/100-step) gaps, plus full torchsave ckpts at 0/50/100.
Writes raw/controls_overlap.csv and prints the table.
"""

import argparse
import csv
import os

from analyze_chunks import scan_zip_payload

SIZES = [4096, 65536, 1048576]
GROUPS = {
    "weights_fp32": [(0, 1), (1, 2), (9, 10), (49, 50), (98, 99), (99, 100),
                     (0, 50), (0, 100), (50, 100)],
    "weights_bf16": [(0, 1), (1, 2), (9, 10), (49, 50), (98, 99), (99, 100),
                     (0, 50), (0, 100), (50, 100)],
    "torchsave": [(0, 50), (0, 100), (50, 100)],
}


def overlap(path_a, path_b):
    aa, _ = scan_zip_payload(path_a, SIZES)
    bb, _ = scan_zip_payload(path_b, SIZES)
    out = {}
    for s in SIZES:
        prior = set(aa[s])
        fps = bb[s]
        hits = sum(1 for h in fps if h in prior)
        out[s] = (hits, len(fps), round(hits / len(fps), 6) if fps else 0.0)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp",
                    default="../../output/checkpoint-dedup/controls")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    raw = os.path.join(args.out, "raw")
    os.makedirs(raw, exist_ok=True)
    rows = []
    for group, pairs in GROUPS.items():
        for a, b in pairs:
            pa = os.path.join(args.inp, group, f"step_{a:03d}.pt")
            pb = os.path.join(args.inp, group, f"step_{b:03d}.pt")
            if not (os.path.exists(pa) and os.path.exists(pb)):
                continue
            for s, (hits, n, ratio) in overlap(pa, pb).items():
                rows.append([group, a, b, s, hits, n, ratio])
                print(f"{group:13s} {a:3d}->{b:3d}  chunk={s:8d}  "
                      f"hit={hits:6d}/{n:6d}  ratio={ratio:.6f}")
    with open(os.path.join(raw, "controls_overlap.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["group", "step_a", "step_b", "chunk_size",
                    "hit_count", "n_chunks", "hit_ratio"])
        w.writerows(rows)


if __name__ == "__main__":
    main()
