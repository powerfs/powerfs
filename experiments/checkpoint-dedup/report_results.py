#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
report_results.py — condense raw CSVs from analyze_chunks.py into the
go/no-go decision numbers (cross-step identical-chunk ratio by chunk size,
steady-state mean over later steps, adjacent-vs-any gap, offset-shift
sensitivity) and emit results_report.md plus machine-readable key_results.json.
"""

import argparse
import csv
import json
import os
from collections import defaultdict


def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results")
    ap.add_argument("--manifest", default="../../output/checkpoint-dedup/manifest.json")
    args = ap.parse_args()
    raw = os.path.join(args.results, "raw")

    metrics = read_csv(os.path.join(raw, "dedup_metrics.csv"))
    shifts = read_csv(os.path.join(raw, "shift_sensitivity.csv"))
    summary = json.load(open(os.path.join(args.results, "summary.json")))

    # steady state: steps 5..19 (exclude warm-up first steps)
    fmt_view_size = defaultdict(lambda: {"any": [], "adj": [], "intra": []})
    for r in metrics:
        if int(r["step"]) < 5:
            continue
        k = (r["format"], r["view"], int(r["chunk_size"]))
        fmt_view_size[k]["any"].append(float(r["hit_any_ratio"]))
        fmt_view_size[k]["adj"].append(float(r["hit_adj_ratio"]))
        fmt_view_size[k]["intra"].append(float(r["intra_dup_ratio"]))

    key = {"steady_state_step5plus": {}, "shift_sensitivity": {}}
    lines = ["# Phase-A results: cross-step chunk redundancy in GPT-2 small checkpoints",
             "",
             "Hit ratio = chunk instances identical to some chunk in an earlier "
             "step (per fixed-size grid); steady-state mean over steps 5-19.",
             ""]
    lines.append("## 1. Cross-step hit ratio")
    lines.append("")
    lines.append("| format | view | chunk | hit-any | hit-prev | intra-dup |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for k in sorted(fmt_view_size, key=lambda x: (x[0], x[1], x[2])):
        v = fmt_view_size[k]
        row = (k[0], k[1], f"{k[2]//1024}K" if k[2] < 1048576 else f"{k[2]//1048576}M",
               mean(v["any"]), mean(v["adj"]), mean(v["intra"]))
        lines.append(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]:.4f} | "
                     f"{row[4]:.4f} | {row[5]:.4f} |")
        key["steady_state_step5plus"][f"{k[0]}/{k[1]}/{k[2]}"] = {
            "hit_any": round(row[3], 6), "hit_prev": round(row[4], 6),
            "intra_dup": round(row[5], 6)}

    # shift: per format/chunk/shift, mean over pairs (steps >=5)
    agg = defaultdict(list)
    for r in shifts:
        if int(r["step"]) < 5:
            continue
        agg[(r["format"], r["view"], int(r["chunk_size"]), int(r["shift"]))].append(
            float(r["hit_ratio"]))
    lines += ["", "## 2. Offset-shift sensitivity (file view, adjacent steps)",
              "",
              "hit ratio when the reference stream is shifted by N bytes "
              "(detects content that is identical but offset-drifting).", "",
              "| format | chunk | shift 1 | 16 | 256 | 4096 |",
              "|---|---:|---:|---:|---:|---:|"]
    fmts = sorted({k[0] for k in agg})
    sizes = sorted({k[2] for k in agg})
    for fmt in fmts:
        for sz in sizes:
            vals = [mean(agg[(fmt, "file", sz, sh)]) for sh in (1, 16, 256, 4096)]
            label = f"{sz//1024}K" if sz < 1048576 else f"{sz//1048576}M"
            lines.append(f"| {fmt} | {label} | " +
                         " | ".join(f"{v:.4f}" for v in vals) + " |")
            for sh, v in zip((1, 16, 256, 4096), vals):
                key["shift_sensitivity"][f"{fmt}/{sz}/{sh}"] = round(v, 6)

    # controls: precision (bf16) and save-gap robustness
    cpath = os.path.join(raw, "controls_overlap.csv")
    if os.path.exists(cpath):
        controls = read_csv(cpath)
        lines += ["", "## 3. Controls: fp32 vs bf16, gap = 1/50/100 steps",
                  "",
                  "| group | gap | chunk | hit ratio |", "|---|---:|---:|---:|"]
        for r in controls:
            if int(r["chunk_size"]) not in (4096, 1048576):
                continue
            gap = int(r["step_b"]) - int(r["step_a"])
            label = f"{int(r['chunk_size'])//1024}K"
            lines.append(f"| {r['group']} | {gap} | {label} | "
                         f"{float(r['hit_ratio']):.6f} |")

    # delta/compression
    dpath = os.path.join(raw, "delta_compression.csv")
    if os.path.exists(dpath):
        deltas = read_csv(dpath)
        lines += ["", "## 4. Adjacent-step similarity beyond exact chunks (zipdata)",
                  "",
                  "| format | pair | exact 4K | zero 4K | bytes equal | "
                  "zstd single | zstd XOR-delta |",
                  "|---|---|---:|---:|---:|---:|---:|"]
        for r in deltas:
            lines.append(
                f"| {r['format']} | {r['step_a']}->{r['step_b']} | "
                f"{float(r['exact_4k_ratio']):.4f} | {float(r['zero_4k_ratio']):.4f} | "
                f"{float(r['byte_equal_ratio']):.3f} | {r['zstd_single_ratio']} | "
                f"{r['zstd_xor_ratio']} |")

    # positive controls: container images and source archives
    ppath = os.path.join(raw, "positive_overlap.csv")
    if os.path.exists(ppath):
        pos = read_csv(ppath)
        lines += ["",
                  "## 5. Positive controls — where byte-level dedup DOES work",
                  "",
                  "Same BLAKE2b-128 fixed-chunk measurement. `perfile` chunks every",
                  "file independently from offset 0 (the POSIX filesystem view);",
                  "`raw` chunks the archive byte stream (misaligned by tar ordering).",
                  "hit-any = fraction of chunks already present in ANY earlier version.",
                  "",
                  "| group | view | version | 4K | 64K | 1M |",
                  "|---|---|---|---:|---:|---:|"]
        cells = {}
        for r in pos:
            cells[(r["group"], r["view"], r["version"], int(r["chunk_size"]))] = \
                float(r["hit_any_ratio"])
        for g in ("rootfs", "source", "images"):
            versions = sorted({r["version"] for r in pos if r["group"] == g})
            for view in ("perfile", "raw", "layers-perfile"):
                for ver in versions[1:] if view != "raw" else versions:
                    ckey = (g, view, ver)
                    vals = [cells.get(ckey + (s,)) for s in (4096, 65536, 1048576)]
                    if vals[0] is None:
                        continue
                    lines.append(f"| {g} | {view} | {ver} | " +
                                 " | ".join(f"{x:.4f}" for x in vals) + " |")
        lpath = os.path.join(raw, "positive_layers.csv")
        if os.path.exists(lpath):
            lays = [r for r in read_csv(lpath)
                    if r["new_vs_prev"] in ("True", "False")]
            base = {}
            for r in lays:
                base.setdefault(r["digest"], []).append(r["version"])
            shared = {d: vs for d, vs in base.items() if len(vs) > 1}
            total_csize = sum(int(r["compressed_size"]) for r in lays
                              if r["version"] == "v3")
            shared_csize = sum(int(r["compressed_size"]) for r in lays
                               if r["digest"] in shared and r["version"] == "v3")
            lines += ["",
                      f"OCI layer blobs (v3 image): {len([r for r in lays if r['version']=='v3'])}; "
                      f"{len(shared)} shared across all 3 versions "
                      f"({shared_csize/1e6:.0f} of {total_csize/1e6:.0f} MB, "
                      "content-addressable, zero transfer/storage). "
                      "Shared digests:"]
            for d, vs in sorted(shared.items()):
                lines.append(f"- `{d}…` in {', '.join(vs)}")

    # go/no-go: 1M file-view torchsave + weights
    lines += ["", "## 6. Go/no-go (decision rules)", ""]
    for fmt in ("torchsave", "weights", "dcp"):
        v1m = key["steady_state_step5plus"].get(f"{fmt}/file/{1048576}", {}).get("hit_any")
        lines.append(f"- {fmt} @1M file view, hit-any = {v1m} "
                     f"(go >0.30, no-go <0.05)")
    lines.append("")

    with open(os.path.join(args.results, "key_results.json"), "w") as f:
        json.dump(key, f, indent=2)
    with open(os.path.join(args.results, "results_report.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
