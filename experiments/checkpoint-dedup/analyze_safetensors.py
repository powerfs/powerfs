#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
analyze_safetensors.py — R2: run the main phase-A cross-step analysis on
the safetensors weights slot via analyze_format(); also reports the
steady-state (steps 5-19) means.

Writes results/raw/safetensors_metrics.csv and
results/raw/safetensors_pair_overlap.csv.
"""

import argparse
import csv
import os

from analyze_chunks import (CHUNK_SIZES, analyze_format, list_steps,
                            pair_overlaps, scan_file)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp",
                    default="../../output/checkpoint-dedup/safetensors")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    raw = os.path.join(args.out, "raw")
    os.makedirs(raw, exist_ok=True)

    files = list_steps_safe(args.inp)
    records = []
    for step, path in files:
        records.append((step, {"file": scan_file(path, CHUNK_SIZES)}, {}))
    metrics, dists, sets = analyze_format("safetensors", records, raw)

    with open(os.path.join(raw, "safetensors_metrics.csv"), "w",
              newline="") as f:
        w = csv.writer(f)
        w.writerow(["format", "view", "chunk_size", "step", "n_chunks",
                    "n_unique", "intra_dup_ratio", "hit_any_count",
                    "hit_any_ratio", "hit_adj_count", "hit_adj_ratio"])
        w.writerows(metrics)
    with open(os.path.join(raw, "safetensors_pair_overlap.csv"), "w",
              newline="") as f:
        w = csv.writer(f)
        w.writerow(["format", "view", "chunk_size", "step_a", "step_b",
                    "inter_over_a", "inter_over_b", "jaccard"])
        pair_overlaps("safetensors", [s for s, _ in files], sets, w)

    # steady-state means
    by_size = {s: {"any": [], "adj": []} for s in CHUNK_SIZES}
    for r in metrics:
        _fmt, _view, size, step = r[0], r[1], int(r[2]), int(r[3])
        if step >= 5:
            by_size[size]["any"].append(float(r[8]))
            by_size[size]["adj"].append(float(r[10]))
    print("safetensors weights, steady state steps 5-19:")
    for s in CHUNK_SIZES:
        a = by_size[s]["any"]
        d = by_size[s]["adj"]
        print(f"  chunk={s:8d} hit-any={sum(a)/len(a):.6f} "
              f"hit-adj={sum(d)/len(d):.6f}")


def list_steps_safe(d):
    out = []
    for name in sorted(os.listdir(d)):
        if name.startswith("step_") and name.endswith(".safetensors"):
            out.append((int(name.split("_")[1].split(".")[0]),
                        os.path.join(d, name)))
    return out


if __name__ == "__main__":
    main()
