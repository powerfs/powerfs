#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
analyze_chunks.py — Phase-A offline chunk redundancy measurement.

Consumes the fixture produced by gen_checkpoints.py and measures byte-level
cross-step redundancy exactly as a fixed-size-chunk client FS would see it.

Views:
  file     : raw on-disk file bytes (the FS view): torchsave/*.pt zip bytes,
             weights/*.pt zip bytes, dcp/step_*/__0_0.distcp bytes
  zipdata  : torch zip container payload only — data/N storage entries
             concatenated per entry (chunks do NOT cross entry boundaries),
             separating tensor bytes from zip/pkl container noise

Metrics per (format, view, chunk_size, step):
  - intra-step duplicates, hit vs any earlier step, hit vs previous step
  - first-seen distance histogram, all-pair unique-fingerprint overlap
  - offset-shift sensitivity: redundancy after shifting the reference stream
    by {1,16,256,4096} bytes (detects "same content, drifted offset" which
    fixed chunking misses; motivation/need for CDC)

Fingerprint: BLAKE2b-128 (collision probability negligible at these corpus
sizes; SHA-256 is used by the production system and is equivalent here).

Outputs (under --out, committed to the repo):
  raw/dedup_metrics.csv, raw/pair_overlap.csv, raw/shift_sensitivity.csv,
  raw/distance_hist.csv, summary.json
"""

import argparse
import csv
import hashlib
import json
import os
import zipfile

CHUNK_SIZES = [4096, 65536, 1048576, 4194304]
SHIFTS = [1, 16, 256, 4096]
READ_BLOCK = 4 * 1024 * 1024


def fp_hash(block):
    return hashlib.blake2b(block, digest_size=16).digest()


class Chunker:
    """Splits one byte stream into fixed-size chunks (first chunk may start
    at `skip` offset, i.e. the shorter prefix is discarded).

    O(n) total: keeps only a sub-chunk carry between pushes; hashing reads
    through zero-copy memoryview slices. (A bytearray with `del buf[:k]`
    would be O(n) per chunk and near-quadratic at 4K granularity.)"""

    __slots__ = ("size", "carry", "started", "skip")

    def __init__(self, size, skip=0):
        self.size = size
        self.carry = b""
        self.started = (skip == 0)
        self.skip = skip

    def push(self, data, out):
        if not self.started:
            data = data[self.skip:]
            self.started = True
        if self.carry:
            data = self.carry + data
            self.carry = b""
        size = self.size
        n = len(data)
        off = 0
        view = memoryview(data)
        while n - off >= size:
            out.append(fp_hash(view[off:off + size]))
            off += size
        if off < n:
            self.carry = bytes(view[off:])


def scan_file(path, sizes, skip_map=None):
    """Return {size: [fingerprints]} for a single file."""
    skip_map = skip_map or {}
    chunkers = {s: Chunker(s, skip_map.get(s, 0)) for s in sizes}
    outs = {s: [] for s in sizes}
    with open(path, "rb") as f:
        while True:
            block = f.read(READ_BLOCK)
            if not block:
                break
            for s, c in chunkers.items():
                c.push(block, outs[s])
    return outs


def scan_zip_payload(path, sizes):
    """Chunk each data/N storage entry independently (no cross-entry chunk)."""
    outs = {s: [] for s in sizes}
    chunkers = {s: Chunker(s) for s in sizes}
    with zipfile.ZipFile(path) as z:
        entries = [
            i for i in z.infolist()
            if os.path.basename(i.filename).split(".")[0].isdigit()
            and os.path.dirname(i.filename).endswith("data")
        ]
        entries.sort(key=lambda i: int(os.path.basename(i.filename)))
        payload_bytes = 0
        for e in entries:
            payload_bytes += e.file_size
            for c in chunkers.values():
                c.carry = b""  # chunks must not cross entry boundaries
            with z.open(e) as ef:
                while True:
                    block = ef.read(READ_BLOCK)
                    if not block:
                        break
                    for s, c in chunkers.items():
                        c.push(block, outs[s])
            for c in chunkers.values():
                c.carry = b""
    return outs, payload_bytes


def list_steps(base, sub, ext=None, is_dir=False):
    d = os.path.join(base, sub)
    out = []
    for name in sorted(os.listdir(d)):
        p = os.path.join(d, name)
        if is_dir:
            if os.path.isdir(p) and name.startswith("step_"):
                out.append((int(name.split("_")[1]), p))
        elif ext and name.endswith(ext):
            out.append((int(name.split("_")[1].split(".")[0]), p))
    return out


def analyze_format(name, steps_with_view, raw_dir):
    """steps_with_view: list of (step, {view: scans dict size->list[fp]},
    auxiliary bytes dict). Produces metric/overlap/distance CSVs (rows
    appended) and returns summary fragment."""
    # accumulators: sets[(view,size)] = list of per-step unique-fp sets;
    # first_seen[(view,size)] = {fp: earliest step}
    sets = {}
    first_seen = {}
    metric_rows = []
    dist_rows = []

    for step, scans, aux in steps_with_view:
        for view, by_size in scans.items():
            for size, fps in by_size.items():
                key = (view, size)
                st = set(fps)
                sets.setdefault(key, []).append(st)
                n = len(fps)
                nuniq = len(st)
                fs = first_seen.setdefault(key, {})
                hit_any = hit_adj = 0
                dist_hist = {}
                prev_set = sets[key][-2] if len(sets[key]) >= 2 else set()
                # per-chunk-instance accounting against earliest-seen map
                for h in fps:
                    first = fs.get(h)
                    if first is not None:
                        dist = step - first
                        if dist >= 1:
                            # cross-step redundancy (distance 0 is an
                            # intra-step duplicate, counted separately)
                            hit_any += 1
                            dist_hist[dist] = dist_hist.get(dist, 0) + 1
                    else:
                        fs[h] = step
                    if h in prev_set:
                        hit_adj += 1
                metric_rows.append([
                    name, view, size, step, n, nuniq,
                    round(1 - nuniq / n, 6) if n else 0,
                    hit_any, round(hit_any / n, 6) if n else 0,
                    hit_adj, round(hit_adj / n, 6) if n else 0,
                ])
                for dist, cnt in sorted(dist_hist.items()):
                    dist_rows.append([name, view, size, step, dist, cnt])

    return metric_rows, dist_rows, sets


# At 4K there are ~7M fingerprints/format; all 190 set intersections would
# cost >1B C-level probes per view. The paper heat-map uses 1M chunks (the
# production chunk size); at 4K we keep only sparse distances {1,2,4,8}.
PAIR_GAPS_4K = {1, 2, 4, 8}


def pair_overlaps(name, ordered_steps, sets, pair_writer):
    for (view, size), per_step_sets in sets.items():
        assert len(per_step_sets) == len(ordered_steps)
        for a in range(len(ordered_steps)):
            b_start = a + 1
            for b in range(b_start, len(ordered_steps)):
                if size == 4096 and (b - a) not in PAIR_GAPS_4K:
                    continue
                sa = per_step_sets[a]
                sb = per_step_sets[b]
                inter = len(sa & sb)
                union = len(sa | sb)
                pair_writer.writerow([
                    name, view, size,
                    ordered_steps[a], ordered_steps[b],
                    round(inter / len(sa), 6) if sa else 0,
                    round(inter / len(sb), 6) if sb else 0,
                    round(inter / union, 6) if union else 0,
                ])


def shift_sensitivity(name, file_lists, shift_writer):
    """file_lists: {view: [(step, path)]}; only file-like views supported."""
    sizes = CHUNK_SIZES
    for view, files in file_lists.items():
        for idx in range(1, len(files)):
            (sp, pp), (sc, pc) = files[idx - 1], files[idx]
            assert sc == sp + 1
            # reference stream chunked for every (size, shift); target once
            ref_outs = {(s, sh): [] for s in sizes for sh in SHIFTS}
            chunkers = {(s, sh): Chunker(s, skip=sh)
                        for s in sizes for sh in SHIFTS}
            with open(pp, "rb") as f:
                while True:
                    block = f.read(READ_BLOCK)
                    if not block:
                        break
                    for key, c in chunkers.items():
                        c.push(block, ref_outs[key])
            tgt = scan_file(pc, sizes)
            for s in sizes:
                for sh in SHIFTS:
                    ref_set = set(ref_outs[(s, sh)])
                    hits = sum(1 for h in tgt[s] if h in ref_set)
                    n = len(tgt[s])
                    shift_writer.writerow([
                        name, view, s, sc, sp, sh, hits, n,
                        round(hits / n, 6) if n else 0,
                    ])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="../../output/checkpoint-dedup")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    base = args.inp
    raw = os.path.join(args.out, "raw")
    os.makedirs(raw, exist_ok=True)

    fm = open(os.path.join(raw, "dedup_metrics.csv"), "w", newline="")
    fp_ = open(os.path.join(raw, "pair_overlap.csv"), "w", newline="")
    fs_ = open(os.path.join(raw, "shift_sensitivity.csv"), "w", newline="")
    fd = open(os.path.join(raw, "distance_hist.csv"), "w", newline="")
    mw = csv.writer(fm)
    pw = csv.writer(fp_)
    sw = csv.writer(fs_)
    dw = csv.writer(fd)
    mw.writerow(["format", "view", "chunk_size", "step", "n_chunks",
                 "n_unique", "intra_dup_ratio", "hit_any_count",
                 "hit_any_ratio", "hit_adj_count", "hit_adj_ratio"])
    pw.writerow(["format", "view", "chunk_size", "step_a", "step_b",
                 "inter_over_a", "inter_over_b", "jaccard"])
    sw.writerow(["format", "view", "chunk_size", "step", "ref_step",
                 "shift", "hit_count", "n_chunks", "hit_ratio"])
    dw.writerow(["format", "view", "chunk_size", "step", "distance", "count"])

    summary = {"fingerprint": "blake2b-128", "chunk_sizes": CHUNK_SIZES,
               "shifts": SHIFTS, "formats": {}}

    # ---- torchsave / weights: file view + zipdata view --------------------
    for fmt, sub in [("torchsave", "torchsave"), ("weights", "weights")]:
        files = list_steps(base, sub, ext=".pt")
        records = []
        payload_total = disk_total = 0
        for step, path in files:
            file_scans = scan_file(path, CHUNK_SIZES)
            zip_scans, payload_bytes = scan_zip_payload(path, CHUNK_SIZES)
            disk_total += os.path.getsize(path)
            payload_total += payload_bytes
            records.append((step, {"file": file_scans,
                                   "zipdata": zip_scans}, {}))
        metrics, dist, sets = analyze_format(fmt, records, raw)
        mw.writerows(metrics)
        dw.writerows(dist)
        ordered = [r[0] for r in records]
        pair_overlaps(fmt, ordered, sets, pw)
        shift_sensitivity(fmt, {"file": files}, sw)
        summary["formats"][fmt] = {
            "n_steps": len(files),
            "disk_bytes": disk_total,
            "zip_payload_bytes": payload_total,
            "container_overhead_ratio": round(1 - payload_total / disk_total, 6),
        }
        print(f"{fmt}: {len(files)} steps analyzed")

    # ---- dcp: __0_0.distcp file view; metadata reported separately --------
    dirs = list_steps(base, "dcp", is_dir=True)
    records = []
    files = []
    meta_total = data_total = 0
    for step, d in dirs:
        dataf = os.path.join(d, "__0_0.distcp")
        metaf = os.path.join(d, ".metadata")
        if os.path.exists(metaf):
            meta_total += os.path.getsize(metaf)
        data_total += os.path.getsize(dataf)
        file_scans = scan_file(dataf, CHUNK_SIZES)
        records.append((step, {"file": file_scans}, {}))
        files.append((step, dataf))
    metrics, dist, sets = analyze_format("dcp", records, raw)
    mw.writerows(metrics)
    dw.writerows(dist)
    pair_overlaps("dcp", [r[0] for r in records], sets, pw)
    shift_sensitivity("dcp", {"file": files}, sw)
    summary["formats"]["dcp"] = {
        "n_steps": len(dirs), "data_bytes": data_total,
        "metadata_bytes": meta_total,
        "metadata_overhead_ratio": round(meta_total / data_total, 6),
    }
    print(f"dcp: {len(dirs)} steps analyzed")

    for f in (fm, fp_, fs_, fd):
        f.close()

    with open(os.path.join(args.out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print("summary + raw CSVs written under", args.out)


if __name__ == "__main__":
    main()
