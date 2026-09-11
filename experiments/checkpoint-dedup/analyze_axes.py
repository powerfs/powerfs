#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
analyze_axes.py — R1: byte-level visibility per redundancy axis (paper F6).

Axes (fixture from gen_axes.py):
  ddp_torch    4 identical full checkpoints            -> expect 1.0
  shard_torch  4 disjoint parameter shards              -> expect ~0
  job_a/job_b  same architecture, different seeds       -> expect ~0
  lora_job1/2  shared base file + distinct tiny adapter -> base 1.0, adapter ~0

Writes results/raw/axes_overlap.csv.
"""

import argparse
import csv
import hashlib
import os

from analyze_chunks import scan_file, scan_zip_payload

SIZES = [4096, 65536, 1048576]


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()[:12]


def views_for(path):
    """{view: {size: [fps]}} for a checkpoint file (zip .pt gets a
    zipdata-entry view too; safetensors/raw files get file view only)."""
    out = {"file": scan_file(path, SIZES)}
    if path.endswith(".pt"):
        try:
            out["zipdata"], _ = scan_zip_payload(path, SIZES)
        except Exception:
            pass
    return out


def compare(label, pair, paths_a, paths_b, writer):
    """paths_*: lists of (artifact, path); each path chunked independently
    (per-file alignment). Returns printed lines."""
    va, vb = {}, {}
    for view in ("file", "zipdata"):
        agg_a, agg_b = {s: set() for s in SIZES}, {s: set() for s in SIZES}
        ok = True
        for (_art, p) in paths_a:
            v = views_for(p)
            if view not in v:
                ok = False
                break
            for s in SIZES:
                agg_a[s].update(v[view][s])
        for (_art, p) in paths_b:
            v = views_for(p)
            if view not in v:
                ok = False
                break
            for s in SIZES:
                agg_b[s].update(v[view][s])
        if not ok:
            continue
        va[view], vb[view] = agg_a, agg_b
        for s in SIZES:
            n = len(agg_b[s])
            hit = len(agg_b[s] & agg_a[s])
            ratio = hit / n if n else 0.0
            writer.writerow([label, pair, view, s, n, hit,
                             round(ratio, 6)])
            print(f"{label:12s} {pair:24s} {view:8s} chunk={s:8d} "
                  f"hit={hit:8d}/{n:8d} ratio={ratio:.4f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp",
                    default="../../output/checkpoint-dedup/axes")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    raw = os.path.join(args.out, "raw")
    os.makedirs(raw, exist_ok=True)
    f = open(os.path.join(raw, "axes_overlap.csv"), "w", newline="")
    w = csv.writer(f)
    w.writerow(["axis", "pair", "view", "chunk_size", "n_chunks",
                "hit", "hit_ratio"])

    base = args.inp

    # axis 1: DDP replicas — rank r vs union of earlier ranks
    for view_mode in ("ddp_torch", "ddp_st"):
        d = os.path.join(base, view_mode)
        ext = ".pt" if view_mode == "ddp_torch" else ".safetensors"
        union = []
        for r in range(4):
            p = os.path.join(d, f"rank{r}{ext}")
            if r > 0:
                compare(view_mode, f"rank{r} vs rank0..{r-1}", union,
                        [(f"rank{r}", p)], w)
            union.append((f"rank{r}", p))
        print(f"[{view_mode}] md5:",
              {f"rank{r}": md5_file(os.path.join(d, f"rank{r}{ext}"))
               for r in range(4)})

    # axis 2: disjoint shards
    d = os.path.join(base, "shard_torch")
    union = []
    for r in range(4):
        p = os.path.join(d, f"rank{r}.pt")
        if r > 0:
            compare("shard", f"rank{r} vs rank0..{r-1}", union,
                    [(f"rank{r}", p)], w)
        union.append((f"rank{r}", p))

    # axis 3: inter-job same architecture
    compare("inter-job", "job_b vs job_a (full.pt)",
            [("full", os.path.join(base, "job_a", "full.pt"))],
            [("full", os.path.join(base, "job_b", "full.pt"))], w)
    compare("inter-job", "job_b vs job_a (weights.safetensors)",
            [("w", os.path.join(base, "job_a", "weights.safetensors"))],
            [("w", os.path.join(base, "job_b", "weights.safetensors"))], w)

    # axis 4: LoRA shared base + distinct adapters, per artifact
    for art in ("base.safetensors", "adapter.safetensors"):
        compare("lora", f"{art}: job2 vs job1",
                [(art, os.path.join(base, "lora_job1", art))],
                [(art, os.path.join(base, "lora_job2", art))], w)
    # and the whole job directory
    compare("lora", "job2 dir vs job1 dir",
            [(x, os.path.join(base, "lora_job1", x))
             for x in ("base.safetensors", "adapter.safetensors")],
            [(x, os.path.join(base, "lora_job2", x))
             for x in ("base.safetensors", "adapter.safetensors")], w)
    f.close()


if __name__ == "__main__":
    main()
