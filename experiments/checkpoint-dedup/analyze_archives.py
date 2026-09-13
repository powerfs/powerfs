#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
analyze_archives.py — POSITIVE controls for byte-level dedup.

Where byte-level fixed-chunk dedup IS expected to win, measured with the
same BLAKE2b-128 / fixed-chunk machinery as analyze_chunks.py:

  groups:
    rootfs   docker export tarballs of posctrl:v1..v3 (flat filesystem view)
    source   git archive tarballs of 5 ordered powerfs commits
    images   docker save tarballs (OCI layout):
               * raw save tarball chunk overlap
               * layer-blob digest sharing (content-addressable units)
               * decompressed layer payload chunk overlap across versions

For every ordered version we report instance hit ratio against (a) the
immediately previous version (hit_adj) and (b) all earlier versions (hit_any).
Writes results/raw/positive_overlap.csv and results/raw/positive_layers.csv.
"""

import argparse
import csv
import gzip
import io
import os
import tarfile

from analyze_chunks import Chunker

SIZES = [4096, 65536, 1048576]
READ_BLOCK = 1 << 20


def scan_stream(iterator, sizes):
    chunkers = {s: Chunker(s) for s in sizes}
    outs = {s: [] for s in sizes}
    for block in iterator:
        if not block:
            break
        for s, c in chunkers.items():
            c.push(block, outs[s])
    return outs


def scan_tar_perfile(path=None, sizes=None, fileobj=None):
    """Chunk every regular file independently from offset 0 (filesystem
    view: fixed blocks are aligned per file, not across the archive)."""
    outs = {s: [] for s in sizes}
    tf = tarfile.open(path, fileobj=fileobj) if path else tarfile.open(fileobj=fileobj)
    for m in tf:
        if not m.isfile():
            continue
        chunkers = {s: Chunker(s) for s in sizes}
        f = tf.extractfile(m)
        while True:
            b = f.read(READ_BLOCK)
            if not b:
                break
            for s, c in chunkers.items():
                c.push(b, outs[s])
    tf.close()
    return outs


def iter_blobs(save_tar):
    """Yield (digest, compressed_size, gzip?) layer/config blobs."""
    with tarfile.open(save_tar) as tf:
        for m in tf.getmembers():
            if not m.isfile() or not m.name.startswith("blobs/sha256/"):
                continue
            digest = m.name.rsplit("/", 1)[-1]
            f = tf.extractfile(m)
            head = f.read(2)
            is_gz = head == b"\x1f\x8b"
            yield digest, m.size, is_gz, head + f.read()


def blob_stream(head_plus, is_gz):
    bio = io.BytesIO(head_plus)
    if is_gz:
        gf = gzip.GzipFile(fileobj=bio)
        while True:
            b = gf.read(READ_BLOCK)
            if not b:
                break
            yield b
    else:
        yield head_plus


def merge(into, per_unit):
    for s, fps in per_unit.items():
        into[s].update(fps)


def overlap_rows(group, view, versions_sets, writer):
    """versions_sets: [(label, {size: set})] ordered oldest->newest."""
    union = {s: set() for s in SIZES}
    prev = None
    for label, sets in versions_sets:
        for s in SIZES:
            cur = sets[s]
            n = len(cur)
            ha = len(cur & union[s]) if union[s] else 0
            hj = len(cur & prev[s]) if prev else 0
            writer.writerow([group, view, label, s, n, hj, ha,
                             round(hj / n, 6) if n else 0.0,
                             round(ha / n, 6) if n else 0.0])
            print(f"{group:8s} {view:8s} {label:6s} chunk={s:8d} "
                  f"n={n:8d} adj={hj/n if n else 0:.4f} "
                  f"any={ha/n if n else 0:.4f}")
        prev = {s: set(cur) for s, cur in sets.items()}
        for s in SIZES:
            union[s].update(sets[s])


def scan_plain_group(directory):
    from analyze_chunks import scan_file
    out = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".tar"):
            continue
        path = os.path.join(directory, name)
        label = name.replace(".tar", "")
        raw = scan_file(path, SIZES)
        out.append((label + "-raw", {s: set(v) for s, v in raw.items()}))
        perfile = scan_tar_perfile(path=path, sizes=SIZES)
        out.append((label + "-perfile",
                    {s: set(v) for s, v in perfile.items()}))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp",
                    default="../../output/positive-controls")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    raw_dir = os.path.join(args.out, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    ov = open(os.path.join(raw_dir, "positive_overlap.csv"), "w", newline="")
    lw = csv.writer(ov)
    lw.writerow(["group", "view", "version", "chunk_size", "n_chunks",
                 "hit_adj", "hit_any", "hit_adj_ratio", "hit_any_ratio"])

    # ---- rootfs & source: raw archive + member-payload views ----
    for group in ("rootfs", "source"):
        d = os.path.join(args.inp, group)
        if not os.path.isdir(d):
            continue
        per_file = scan_plain_group(d)
        # split into two ordered view series
        for view, suffix in (("raw", "-raw"), ("perfile", "-perfile")):
            series = [(l[:-len(suffix)], s) for l, s in per_file
                      if l.endswith(suffix)]
            overlap_rows(group, view, series, lw)

    # ---- images: docker save OCI layout ----
    idir = os.path.join(args.inp, "images")
    layer_f = open(os.path.join(raw_dir, "positive_layers.csv"), "w", newline="")
    layer_w = csv.writer(layer_f)
    layer_w.writerow(["version", "digest", "compressed_size",
                      "is_gzip", "new_vs_prev"])
    if os.path.isdir(idir):
        seen_before = set()
        layer_cache = {}        # digest -> {size: set} (decompressed)
        layer_csize = {}        # digest -> compressed bytes
        img_series = []
        for name in sorted(os.listdir(idir)):
            if not name.endswith(".tar"):
                continue
            label = name.replace("save_", "").replace(".tar", "")
            from analyze_chunks import scan_file
            raw = scan_file(os.path.join(idir, name), SIZES)
            img_series.append((label, {s: set(v) for s, v in raw.items()}))
            layer_sets = {s: set() for s in SIZES}
            digests_here = set()
            for digest, csize, is_gz, blob in iter_blobs(
                    os.path.join(idir, name)):
                data = b"".join(blob_stream(blob, is_gz))
                if len(data) < 100 * 1024:
                    # config json / manifests: not filesystem layers
                    layer_w.writerow([label, digest[:19], csize,
                                      int(is_gz), "config"])
                    continue
                digests_here.add(digest)
                layer_csize[digest] = csize
                if digest not in layer_cache:
                    try:
                        per = scan_tar_perfile(fileobj=io.BytesIO(data),
                                               sizes=SIZES)
                        layer_cache[digest] = {s: set(v)
                                               for s, v in per.items()}
                    except tarfile.TarError:
                        layer_cache[digest] = {s: set() for s in SIZES}
                merge(layer_sets, layer_cache[digest])
                layer_w.writerow([label, digest[:19], csize, int(is_gz),
                                  str(digest not in seen_before)])
            img_series.append((label + "-layers", layer_sets))
            seen_before = digests_here
        overlap_rows("images", "raw",
                     [(l, s) for l, s in img_series if not l.endswith("-layers")],
                     lw)
        overlap_rows("images", "layers-perfile",
                     [(l[:-len("-layers")], s) for l, s in img_series
                      if l.endswith("-layers")], lw)
    layer_f.close()
    ov.close()


if __name__ == "__main__":
    main()
