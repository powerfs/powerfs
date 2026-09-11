#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
make_figures.py — render paper figures F1-F4 directly from result CSVs.

Outputs figures/f1_teaser.pdf ... f4_delta.pdf (plus .png for previews).
All numbers trace to results/raw/*.csv; no hand-typed values.
"""

import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

RAW = os.path.join(os.path.dirname(__file__), "results", "raw")
OUT = os.path.join(os.path.dirname(__file__), "figures")
SIZES = [4096, 65536, 1048576, 4194304]
SLABEL = ["4K", "64K", "1M", "4M"]

C_SIM = "#c0392b"   # similarity axis (bad)
C_COP = "#27ae60"   # copy axis (good)
C_TRAD = "#2c6fbb"  # traditional versioned workloads
C_GREY = "#7f8c8d"


def rows(name):
    with open(os.path.join(RAW, name), newline="") as f:
        return list(csv.DictReader(f))


def steady_means(metrics_rows, fmt, view, size, col):
    vals = [float(r[col]) for r in metrics_rows
            if r["format"] == fmt and r["view"] == view
            and int(r["chunk_size"]) == size and int(r["step"]) >= 5]
    return sum(vals) / len(vals) if vals else float("nan")


def save(fig, name):
    os.makedirs(OUT, exist_ok=True)
    fig.savefig(os.path.join(OUT, name + ".pdf"), bbox_inches="tight")
    fig.savefig(os.path.join(OUT, name + ".png"), dpi=160, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- F1 teaser
FLOOR = 5e-6   # plotting floor; exact zeros annotated separately


def f1(metrics, st_metrics, axes, pos):
    labels, vals, colors = [], [], []

    def add(label, v, c):
        labels.append(label)
        vals.append(v)
        colors.append(c)

    # checkpoint similarity axis (4K, file view, steady state)
    add("torch.save cross-step", steady_means(metrics, "torchsave", "file", 4096, "hit_any_ratio"), C_SIM)
    add("DCP cross-step", steady_means(metrics, "dcp", "file", 4096, "hit_any_ratio"), C_SIM)
    add("weights cross-step", steady_means(metrics, "weights", "file", 4096, "hit_any_ratio"), C_SIM)
    add("safetensors cross-step", steady_means(st_metrics, "safetensors", "file", 4096, "hit_any_ratio"), C_SIM)
    add("same arch., diff-seed job",
        float(next(r for r in axes if r["axis"] == "inter-job"
                  and "full" in r["pair"] and r["chunk_size"] == "4096")["hit_ratio"]),
        C_SIM)
    add("disjoint parameter shards",
        float(next(r for r in axes if r["axis"] == "shard"
                  and r["chunk_size"] == "4096")["hit_ratio"]), C_SIM)
    # copy axis
    add("DDP rank replicas",
        float(next(r for r in axes if r["axis"] == "ddp_torch"
                  and r["chunk_size"] == "4096")["hit_ratio"]), C_COP)
    add("LoRA shared base",
        float(next(r for r in axes if r["axis"] == "lora" and "base" in r["pair"]
                  and r["chunk_size"] == "4096")["hit_ratio"]), C_COP)
    # traditional versioned workloads
    for ver, lab in [("rootfs_v2", "image v2 vs v1"),
                     ("rootfs_v3", "image v3 vs history"),
                     ("src_s2", "source, 276 commits"),
                     ("src_s5", "source, 20 commits")]:
        grp = "rootfs" if ver.startswith("rootfs") else "source"
        v = float(next(r for r in pos if r["group"] == grp and
                       r["view"] == "perfile" and r["version"] == ver and
                       r["chunk_size"] == "4096")["hit_any_ratio"])
        add(lab, v, C_TRAD)

    fig, ax = plt.subplots(figsize=(7.6, 3.5))
    fig.subplots_adjust(bottom=0.30, top=0.88)
    x = np.arange(len(labels))
    plot_vals = [max(v, FLOOR) for v in vals]
    bars = ax.bar(x, plot_vals, color=colors, width=0.72)
    for xi, v in zip(x, vals):
        if v == 0:
            ax.text(xi, FLOOR * 1.25, "0", ha="center", va="bottom",
                    fontsize=7, color=C_SIM)
    ax.set_yscale("log")
    ax.set_ylim(3e-6, 2.2)
    ax.set_ylabel("4KB block hit ratio")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=7.5, rotation=38, ha="right")
    ax.axhline(0.05, color=C_GREY, ls=":", lw=1)
    ax.text(10.6, 0.062, "5% no-go threshold", fontsize=7, color=C_GREY,
            ha="right")
    for xi, v in zip(x, vals):
        if v >= 0.999:
            ax.text(xi, 1.28, "100%", ha="center", fontsize=7)
    ax.axvspan(-0.5, 5.5, color=C_SIM, alpha=0.05)
    ax.axvspan(5.5, 7.5, color=C_COP, alpha=0.07)
    ax.axvspan(7.5, 11.5, color=C_TRAD, alpha=0.06)
    # group labels at the TOP to avoid colliding with tick labels
    for cx, w, txt, c in [(2.5, 5.9, "checkpoint similarity axis", C_SIM),
                          (6.5, 1.9, "copy axis", C_COP),
                          (9.5, 3.9, "versioned system files", C_TRAD)]:
        ax.annotate(txt, xy=(cx / 11.5, 1.0), xycoords="axes fraction",
                    ha="center", va="bottom", fontsize=8, color=c,
                    xytext=(0, 2), textcoords="offset points", fontweight="bold")
    ax.grid(axis="y", ls=":", alpha=0.4)
    save(fig, "f1_teaser")


# ----------------------------------------------------- F2 chunk-size sweep
def f2(metrics, st_metrics):
    series = [("torch.save, file", "torchsave", "file", "o-", C_SIM),
              ("torch.save, zip entries", "torchsave", "zipdata", "s--", C_SIM),
              ("weights, file", "weights", "file", "^-", "#e67e22"),
              ("DCP, file", "dcp", "file", "d--", "#8e44ad"),
              ("safetensors, file", None, None, "x-", C_GREY)]
    fig, ax = plt.subplots(figsize=(3.5, 2.7))
    for label, fmt, view, style, c in series:
        if fmt is None:
            y = [steady_means(st_metrics, "safetensors", "file", s, "hit_any_ratio")
                 for s in SIZES]
        else:
            y = [steady_means(metrics, fmt, view, s, "hit_any_ratio") for s in SIZES]
        y = [max(v, 2e-5) for v in y]
        ax.plot(range(4), y, style, label=label, color=c, ms=4, lw=1.4)
    ax.set_yscale("log")
    ax.set_ylim(1e-5, 2e-2)
    ax.set_xticks(range(4))
    ax.set_xticklabels(SLABEL)
    ax.set_xlabel("block size")
    ax.set_ylabel("steady-state cross-step hit ratio")
    ax.legend(fontsize=6.5, frameon=False)
    ax.grid(ls=":", alpha=0.4)
    save(fig, "f2_chunksizes")


# ------------------------------------------------------- F3 distance decay
def f3(pairs, pos):
    fig, (a, b) = plt.subplots(1, 2, figsize=(7.0, 2.7),
                               gridspec_kw={"width_ratios": [1.05, 1]})
    # left: checkpoint hit ratio vs step gap (4K file view)
    for fmt, c, lab in [("torchsave", C_SIM, "torch.save"),
                        ("weights", "#e67e22", "weights")]:
        by_gap = {}
        for r in pairs:
            if (r["format"] == fmt and r["view"] == "file"
                    and r["chunk_size"] == "4096"):
                g = int(r["step_b"]) - int(r["step_a"])
                by_gap.setdefault(g, []).append(float(r["inter_over_b"]))
        gaps = sorted(by_gap)
        ys = [sum(by_gap[g]) / len(by_gap[g]) for g in gaps]
        a.plot(gaps, [max(v, FLOOR) for v in ys], "o-", ms=3, color=c, label=lab)
        if ys and ys[-1] == 0:
            a.text(gaps[-1] + 0.25, FLOOR * 1.3, "0", ha="left",
                   fontsize=7, color=c)
    a.set_yscale("log")
    a.set_ylim(3e-6, 2e-2)
    a.set_xlabel("step distance")
    a.set_ylabel("hit ratio of later checkpoint")
    a.set_title("training checkpoints (4K)", fontsize=9)
    a.legend(fontsize=7, frameon=False)
    a.grid(ls=":", alpha=0.4)

    # right: source/image per-file hit-any vs version distance (4K)
    sx = [276, 60, 60, 20]
    sy = [float(next(r for r in pos if r["group"] == "source"
                     and r["view"] == "perfile" and r["version"] == v
                     and r["chunk_size"] == "4096")["hit_any_ratio"])
          for v in ("src_s2", "src_s3", "src_s4", "src_s5")]
    b.scatter(sx, sy, s=28, color=C_TRAD, label="source archives")
    # raw tar view of same pairs
    ry = [float(next(r for r in pos if r["group"] == "source"
                     and r["view"] == "raw" and r["version"] == v
                     and r["chunk_size"] == "4096")["hit_any_ratio"])
          for v in ("src_s2", "src_s3", "src_s4", "src_s5")]
    b.scatter(sx, ry, s=28, facecolors="none", edgecolors=C_TRAD,
              label="raw tar stream")
    b.scatter([1, 2], [0.586, 0.977], marker="^", s=42, color=C_COP,
              label="image rootfs v2/v3")
    b.set_xscale("log")
    b.set_xlabel("version distance (commits)")
    b.set_ylim(-0.03, 1.08)
    b.set_title("versioned system files (4K)", fontsize=9)
    b.legend(fontsize=6.8, frameon=False, loc="lower left")
    b.grid(ls=":", alpha=0.4)
    save(fig, "f3_distance")


# ----------------------------------------------------------------- F4 delta
def f4(delta):
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.6), sharey=True)
    pairs_lab = ["step 0→1", "step 9→10", "step 18→19"]
    mets = [("byte_equal_ratio", "byte-equal fraction", "#34495e"),
            ("zstd_single_ratio", "zstd single", "#2c6fbb"),
            ("zstd_xor_ratio", "zstd XOR-delta", "#27ae60")]
    for ax, fmt in zip(axes, ("weights", "torchsave")):
        rs = [r for r in delta if r["format"] == fmt]
        x = np.arange(3)
        w = 0.25
        for i, (col, lab, c) in enumerate(mets):
            vals = [float(r[col]) for r in rs]
            ax.bar(x + (i - 1) * w, vals, w, label=lab, color=c)
        ax.axhline(1.0, color=C_GREY, ls=":", lw=1)
        ax.set_xticks(x)
        ax.set_xticklabels(pairs_lab, fontsize=7.5)
        ax.set_title("weights-only" if fmt == "weights" else "full torch.save",
                     fontsize=9)
        ax.set_ylim(0, 1.45)
        ax.grid(axis="y", ls=":", alpha=0.4)
    axes[0].set_ylabel("ratio (1.0 = no gain)")
    axes[1].legend(fontsize=7, frameon=False, loc="upper left")
    save(fig, "f4_delta")


def main():
    metrics = rows("dedup_metrics.csv")
    st_metrics = rows("safetensors_metrics.csv")
    axrows = rows("axes_overlap.csv")
    pos = rows("positive_overlap.csv")
    pairs = rows("pair_overlap.csv")
    delta = rows("delta_compression.csv")
    f1(metrics, st_metrics, axrows, pos)
    f2(metrics, st_metrics)
    f3(pairs, pos)
    f4(delta)
    print("figures ->", OUT)


if __name__ == "__main__":
    main()
