# Where Byte-Level Checkpoint Deduplication Dies: A Boundary Study of Semantic Redundancy in AI Training Checkpoints

**Draft v0.1 — 2026-09-11 — target: HotStorage'27 (measurement + position)**

> **Venue format note (2026-09):** HotStorage is now an **ACM** workshop
> (18th edition, in cooperation with USENIX; proceedings in the ACM DL).
> Per the HotStorage'26 CFP: acmart sigconf 10pt two-column, **5 content
> pages excluding references**, double-blind, Position papers carry a
> "Position:" title prefix and declare the type in HotCRP. Re-verify
> against the HotStorage'27 CFP (spring 2027). Submission LaTeX lives in
> `docs/hotstorage27/` (main.tex/refs.bib, compiled 6 pp. incl. refs);
> this markdown file is the unabridged v0.1 text source.
>
> Drafting notes (delete before submission): all numbers are reproduced by
> scripts and CSVs in `experiments/checkpoint-dedup/`; figure placeholders
> [F1]…[F4] map to the outline figure list. References verified from
> official proceedings pages 2026-09.

## Abstract

Semantic checkpoint-deduplication systems report 6–896× storage savings by
reasoning about *tensors*—identifying replicated, architecturally shared,
or slowly changing parameters across ranks, jobs, and training steps.
All of them require framework cooperation: offline tensor analysis,
specialised grouped-I/O APIs, or accelerator-offloaded hashing. It is
tempting to assume that an unmodified POSIX filesystem could capture the
same savings with transparent, content-defined block deduplication. We
test this assumption with one fixed measuring rod—BLAKE2b-128 fingerprints
over fixed-size blocks—applied to real (CPU) GPT-2 small training runs in
three checkpoint formats, precision and save-gap controls, synthetic
multi-rank and multi-job fixtures, and, as positive controls, container
image layers and multi-version source archives. The boundary is sharp:
the byte layer sees the **copy axis** (DDP rank replicas, shared LoRA base
files) at 100% hit ratio, but sees essentially **nothing** on the
**similarity axis**—cross-step temporal redundancy (≤0.29% of 1 MB chunks,
all zero blocks), same-architecture different-seed jobs (~0%), and disjoint
parameter shards (0%)—even though a general compressor fares no better
(zstd 1.08×; XOR-delta ≤1.28×). The same code path scores 59–98% on image
layers and source trees. We argue the right role for a transparent layer
is *learn-to-skip*: classify workloads before touching bytes, with
asymmetric loss that can only ever affect performance, never correctness.

## 1 Introduction

Checkpoints already dominate AI-cluster storage traffic, and the problem
grows with model size and checkpoint frequency. Two FAST'26 systems attack
the waste semantically: AdaCheck [1] classifies checkpoint tensors as
replicated, architecturally shared, or temporally similar, and reports
6–896× savings; AITURBO [2] batches tensor I/O into grouped operations and
computes BLAKE3 [3] fingerprints with optimised XPU kernels because CPU
hashing cannot keep up with the storage fabric. The common denominator is
cooperation: the application must expose tensor identity or use a
specialised API. The closest byte-level predecessor, Kaiser et al. [4],
characterised block-deduplication potential in *HPC* application
checkpoints in 2016 and found application-dependent but real recurring
savings—raising the question whether modern AI-training checkpoints behave
the same; our answer is that they do not.

Meanwhile, transparent block deduplication is a textbook POSIX-filesystem
feature [5,6]: fingerprint content blocks, suppress redundant writes, and
leave the application untouched. If semantic checkpoint redundancy were
visible at the byte layer, *unmodified* PyTorch jobs would benefit simply
by mounting the filesystem—no framework changes, no XPU. We asked exactly
one question:

> **Of the redundancy that tensor-level systems exploit, how much reaches
> an unmodified POSIX byte layer?**

We answer with a controlled measurement on a self-contained, reproducible
GPT-2 small (124 M parameter) training fixture—real forward/backward and
AdamW updates, not synthesised tensors—plus axis-specific fixtures and two
workload families where byte deduplication is *expected* to win.

**Findings in one paragraph.** Cross-step identical 1 MB blocks in normal
training checkpoints are 0.14% (`torch.save` full state) to 0.28%
(Distributed Checkpoint) and **0%** for weights-only files; every observed
hit is a zero block. Offset sweeps rule out alignment drift; bf16 weights
and 50/100-step gaps do not help; zstd and XOR-delta compressors cannot
exploit the data either. But the byte layer is not blind in general:
identical DDP rank replicas and shared LoRA base files match at 100%, while
container image layers and source archives score 59–98% under the same
code. Redundancy splits by **axis**, not by workload.

**Contributions.**

1. A controlled, reproducible study of semantic-redundancy visibility at
   the POSIX byte layer across three checkpoint formats, two precisions,
   three save gaps, and all three semantic redundancy axes.
2. A mechanism explanation—dense gradients plus decoupled weight decay make
   every fp32 byte change every step—supported by controls that rule out
   chunking, alignment, precision, and temporal distance as explanations.
3. A sharp positive/negative boundary (copies visible, similarities
   invisible) and its design implication: transparent storage should
   *learn to skip* hashing on similarity-dominated workloads, under an
   asymmetric-loss contract that keeps ML out of the correctness path.

## 2 Background and Redundancy Taxonomy

**Checkpoint layouts.** `torch.save` serialises a pickle object inside an
uncompressed (STORED) zip container; tensor storage lands in numbered
`data/N` entries. `torch.distributed.checkpoint` (DCP) writes one or more
`.distcp` shard files plus metadata in the same zip-based format.
safetensors [7] instead stores tensors as one contiguous little-endian byte
region preceded by a JSON header. None of these formats compresses fp32
state, because it does not compress (§4.3).

**Byte-level deduplication.** A transparent client partitions write
streams into fixed-size blocks, fingerprints each (we use BLAKE2b-128), and
suppresses a block when the fingerprint already exists. Cost is dominated
by hashing—exactly why AITURBO computes BLAKE3 [3] on XPU kernels [2]—and by
block alignment: fixed grids miss identical content that drifts in offset,
which content-defined chunking [8,9] addresses at extra CPU cost. Block
stores from Venti [6] to iDedup [5] operate on this model; iDedup also
documented
that deduplication fragments physical layout and can hurt reads.

**Three redundancy axes.** Semantic systems exploit three structurally
different sources of repeated bytes:

| Axis | Example | Who benefits today |
|---|---|---|
| **A. Intra-job copies** | DDP replicates all parameters/optimizer state per rank; TP replicates embeddings | Any byte-identical copy mechanism |
| **B. Inter-job similarity** | Same architecture, different data/seed; LoRA jobs share one frozen base | Tensor-keyed systems [1] |
| **C. Inter-step similarity** | Consecutive checkpoints of one job | Tensor diff / grouped-I/O [1,2] |

The crucial distinction is **copies versus similarities**. A copy is the
same tensor placed in several files; the bytes are identical. A similarity
is *the same parameter* in two semantic versions—updated by an optimizer,
or initialised from a different seed. Semantic tensor keys (name, shape,
allocation identity) recognise the latter; byte fingerprints can only match
exact content. Table 1 is the taxonomy; Table 2 (§4.5) fills in what the
byte layer actually sees on each axis.

## 3 Methodology

**Fixture (inter-step axis, C).** We implement a self-contained GPT-2
small (50,257 vocab, 1,024 positions, 768 width, 12 heads, 12 layers; tied
input/output embeddings; pre-LayerNorm; HF-compatible tensor naming) and
train it with AdamW (lr=1e-3, betas=(0.9,0.999), eps=1e-8, weight
decay=0.01) on random token streams (batch 1, sequence 128) for 20 steps,
saving every step in three layouts: full `torch.save`
(model+optimizer+step+loss; 1,493 MB), weights-only (498 MB), and DCP
(1,494 MB)—about 65 GB total. A 100-step run provides long-gap and precision
controls. torch 2.4.1+cpu on Python 3.8; seed 20260910; every script is
checked in and re-runnable. We deliberately use a small CPU model: the
mechanism under test (element-wise optimizer updates) is independent of
parameter count, and a controlled fixture lets us isolate each alternative
explanation in turn.

**Analyzer.** For every checkpoint we fingerprint fixed blocks of
{4 KB, 64 KB, 1 MB, 4 MB} using BLAKE2b-128. We report two views: the raw
*file* view and a *zipdata* view where every `data/N` entry is chunked
independently (no blocks cross entry boundaries), matching entry-aware
clients. Metrics: per-chunk-instance hit ratio against the immediately
previous version (*hit-prev*) and against the union of all earlier versions
(*hit-any*), all-pairs Jaccard, first-seen distance histograms, and
offset-shift sweeps that re-chunk the reference stream after skipping
{1,16,256,4096} bytes—our detector for "identical content, drifted offset",
the condition content-defined chunking would rescue. An independent
synthetic fixture (known random/zero/pattern segments) validates the
analyzer: exact instance counts match construction.

**Axis fixtures (A and B).** From two independent 3-step runs (seeds
20260910 and 20260911) we materialise: four byte-identical full-checkpoint
*rank replicas* per format (md5-verified) and four disjoint 1/4 parameter
*shards*; same-architecture cross-job pairs; and two LoRA-style jobs
comprising one identical 498 MB base file plus distinct 0.6 MB adapters.

**Positive controls.** Three images built sequentially on ubuntu:20.04
(ca-certificates; git/python; editing tools), exported as flat rootfs
tarballs and as `docker save` OCI archives, and `git archive` tarballs of
five powerfs repository commits spanning ~276, 60, 60, and 20 commits.
The image *application* layers are deliberately **full sibling layers**,
not an optimised incremental chain—a harsher control than real registries.
For archives we additionally measure a *per-file* view (each tar member
chunked from offset zero, as a POSIX client stores it) and the naive *raw*
tar-stream view.

**Scope.** This is a controlled microbenchmark, not field-trace analysis.
We claim a mechanism result and a boundary result; absolute ratios at
hyperscale, multi-node timing, and non-AdamW optimisers are out of scope
(§7).

## 4 Findings

### 4.1 F1 — Temporal redundancy is invisible at the byte layer

Steady state (steps 5–19) cross-step hit-any ratios:

| Layout / view | 4 KB | 64 KB | 1 MB | 4 MB |
|---|---:|---:|---:|---:|
| `torch.save` / file | 0.0037 | 0.0036 | **0.0014** | 0 |
| `torch.save` / zipdata | 0.0037 | 0.0037 | 0.0029 | 0 |
| weights-only / file | 0 | 0 | **0** | 0 |
| DCP / file | 0.0037 | 0.0036 | 0.0028 | 0 |

[F2: lines across chunk size; note zero-block annotation.] Every hit in
the full-checkpoint rows is an all-zero block: exact-block ratio equals
zero-block ratio (0.37%) in our delta analysis, and weights-only files—no
optimizer slots, no padding zero blocks—score exactly zero at every size.
At 1 MB, all-pairs Jaccard across steps 0–19 is a flat 0.000351 regardless
of step distance: the only overlap is the same persistent zero blocks,
present in every checkpoint. Temporal similarity—what AdaCheck and AITURBO
harvest step after step—simply does not exist as equal bytes. Notably,
AITURBO's finest detection granularity is 4 MB [2]; at exactly that setting
every format in our fixture yields zero cross-step hits—XPU-accelerated
hashing would be spent on a stream containing nothing to find.

### 4.2 F2 — Not an alignment problem: boundary alignment is necessary but not sufficient

Two complementary tests. *Within* checkpoints, shifting the previous
checkpoint's chunk grid by 1, 16, or 256 bytes leaves the hit ratio
unchanged (e.g. 0.001404 at 1 MB for every shift; a 4096 shift removes the
prefix chunk and nothing else). Identical-but-drifting content, the failure
mode content-defined chunking fixes, is absent: there is no hidden
redundancy for CDC to uncover.

*Across* workload families, alignment matters sharply. Chunking archive
byte streams naively (raw tar view) collapses even our positive controls:
rootfs 4 KB hits drop from 58.6% to 3.9% (v2) and from 97.7% to 10.4%
(v3); source archives drop from 94.8% to 54.3% in the closest pair and to
1.4–4.5% at moderate distances; at 1 MB every raw-stream pair is zero. Tar
member reordering and headers destroy grid alignment. But checkpoint
zipdata entries *are* chunked from independent aligned boundaries—and still
score zero. Alignment is therefore necessary (the controls prove the
instrument can read zero when boundaries are wrong) but not sufficient
(the checkpoints prove aligned boundaries alone do not create stability).

### 4.3 F3 — Not precision, age, or compressibility

*Precision control.* bf16 weights across 100 steps yield 6–11 matching
4 KB blocks out of 60,726 (≤0.018%) and zero at 64 KB and 1 MB; fp32
weights are exactly zero at gaps of 1, 50, and 100 steps. Lower mantissa
precision does not quantise Adam updates into byte-identical storage at
useful block sizes.

*Format control.* A safetensors run replaying the **identical** 20-step
training sequence (bit-identical loss trajectory: 10.989, 10.916, 10.968,
…) scores 0.0025% steady-state hits at 4 KB and zero at 64 KB/1 MB/4 MB.
The result is a property of the bytes, not of the zip container.

*Compression control.* Perhaps redundancy exists in a form blocks cannot
express. zstd-1 on a single checkpoint compresses 1.08×; an XOR-delta
stream between adjacent steps compresses only 1.15–1.28×. Byte-wise
equality between adjacent steps is just 19.8–30.3%, rising slowly with
step count. General-purpose encoders confirm the block analyzer: fp32
training state in motion carries almost no extractable redundancy.

### 4.4 F4 — Mechanism: every byte is touched every step

The zero result has a precise mechanism. (1) AdamW applies decoupled weight
decay: every parameter element receives `p ← p − lr·wd·p` each step
whenever it participates in the optimizer, a relative change of 10⁻⁵—small
in value but guaranteed to flip fp32 low bytes. (2) Adam first/second
momentum slots decay and accumulate per element. (3) In our tied-head GPT-2,
the lm-head backward produces a dense gradient through the whole token
embedding matrix; and the embedding backward materialises **zero (not
`None`) gradients** for unsampled rows, so weight decay touches those rows
too—we verified directly that even embedding rows unused by the current
batch change bytes every step. The union is that, at fp32, essentially
every parameter byte changes on every optimizer step. Tensor-level systems
do not match bytes: they match *keys* (parameter names, shapes, producer
identity), which stay constant while the bytes churn. That is the whole
explanation for the axis boundary.

### 4.5 F5/F6 — The redundancy-axis map: copies visible, similarities invisible

Table 2 (per-file aligned blocks, same trained states):

| Axis | Comparison | 4 KB | 64 KB | 1 MB |
|---|---|---:|---:|---:|
| A copies | DDP rank replicas, `torch.save` | 1.000 | 1.000 | 1.000 |
| A copies | DDP rank replicas, safetensors | 1.000 | 1.000 | 1.000 |
| A divides | Disjoint FSDP/TP shards vs earlier shards | 0 | 0 | 0 |
| B cross-job | full ckpt, different seed | 0.000 | 0.000 | 0.0007 |
| B cross-job | weights safetensors, different seed | 0 | 0 | 0 |
| B LoRA | shared base file, job2 vs job1 | 1.000 | 1.000 | 1.000 |
| B LoRA | distinct adapter files | 0 | 0 | — |
| B LoRA | whole-job directories | 0.9988 | 0.9988 | 1.000 |
| C temporal | consecutive steps (F1) | 0.0037* | 0.0036* | 0–0.0029 |

\*hits are zero blocks. The single 1 MB inter-job hit (1 of 1,423 blocks)
is hash coincidence/zero content, not structure.

The pattern is unambiguous. Whole-object copies—rank replicas and shared
base files—collapse to one physical copy transparently; the LoRA directory
result (99.9%) says a transparent store already handles the dominant bytes
of the LoRA sharing pattern, missing only the 0.6 MB adapters. Everything
that requires recognising "the same parameter in another version"—consecutive
steps, independent jobs, disjoint shards—is invisible. The 6–896× headline
numbers of semantic systems live on this invisible side; their framework
requirements are not an accident of engineering, they are forced by where
the redundancy is.

### 4.6 Positive controls: the same instrument scores 59–98% where it should

[F1 teaser / F3 distance curves.]

| Workload (per-file view, hit-any) | 4 KB | 64 KB | 1 MB |
|---|---:|---:|---:|
| Image rootfs v2 vs v1 | 0.586 | 0.623 | 0.660 |
| Image rootfs v3 vs v1+v2 | **0.977** | 0.974 | **0.981** |
| Source, ~276 commits apart | 0.324 | 0.115 | — |
| Source, moderate gaps | 0.705 / 0.754 | 0.563 / 0.585 | — |
| Source, ~20 commits apart | **0.948** | **0.956** | — |
| Training checkpoints (all formats) | **0** | 0 | 0 |

At the OCI layer level, the 75 MB ubuntu base blob is content-addressable
and shared verbatim across all three images; more tellingly, even the full
191 MB and 197 MB sibling application layers—built by reinstalling and
upgrading packages—contain 59% and 98% of blocks already seen in earlier
versions, because dpkg overwrites the same library and binary files with
identical content. System files are byte-stable across versions; optimizer
state is byte-stable for exactly one step. With the same fingerprint
function, same block sizes, and same analysis code scoring 98% and 0%, the
zero cannot be blamed on the instrument.

[F4: byte-equality ratio over step distance plus zstd/XOR-delta ratios.]

## 5 Implications: Learn to Skip, Not to Deduplicate

**A classifier before the hash.** If hashing is expensive (AITURBO's XPU
is the admission [2]) and temporal checkpoints never hit, a transparent
client should not try to be smarter about *how* it hashes checkpoint
streams—it should decide *whether to hash at all*, using signals available
before touching bytes: write path (periodic large sequential file from a
training job), file size and age pattern, directory topology (rank
replicas), and cheap prefix probes. Copy-heavy paths (rank replicas, base
models, image pulls, log/tar bundles) should always hash; churn-heavy
training state should skip to write-through.

**Asymmetric loss keeps ML off the correctness path.** A gating classifier
makes exactly one mistake in each direction: a false positive (hashed a
unique stream) wastes fingerprints; a false negative (skipped a redundant
stream) loses one saving. Both errors affect performance only. Suppression
still requires an exact fingerprint match, so the neural component can
neither corrupt data nor lose a byte. This is the dividing line against
prediction-in-the-data-path systems such as LinnOS [10] or KML's learned
kernel heuristics [11]: prediction proposes, exact matching disposes.
Our measurements supply what such a gate previously lacked—labelled
**negative** examples of production-shaped workloads; earlier write-side
prediction work was tuned on traditional workloads (logs, overwrites,
versioned files), where redundancy is the default rather than the
exception.

**A lower bound, not an obituary, for semantic systems.** The zero on the
similarity axis quantifies the *necessity* of tensor cooperation: no amount
of transparent cleverness—CDC, larger blocks, compression—can intercept
savings whose identical units are tensor keys rather than bytes. Conversely,
the 100% on the copy axis shows transparent dedup is still the right
mechanism for replicated saves and shared bases; a deployed system should
compose both, with the byte layer handling copies and a semantic channel
(AdaCheck-style) handling similarities.

**Existence proof.** In our POSIX research filesystem (PowerFS), an inline
kernel content-hash fast path with a gated write needle reduces dedup
metadata RPCs from 1,024 to 258 per 256 MB write round and improves
steady-state write bandwidth by 50% across four rounds with md5-identical
output—demonstrating that the gated path itself is cheap and safe; the
measurement in this paper determines *where it must not be switched on*.

## 6 Related Work

**Semantic checkpoint storage.** AdaCheck [1] classifies and rewrites
tensors across parallelism, architecture, and temporal dimensions;
AITURBO [2] contributes grouped I/O and XPU BLAKE3 fingerprinting. Kaiser
et al. [4] is the closest predecessor: a byte-level measurement of
deduplication potential across HPC application checkpoints, where static
memory regions and repeated arrays produced application-dependent savings.
We revisit the same question for modern AI-training state under AdamW,
decompose redundancy into copy and similarity axes instead of a single
ratio, and show the answer flips for the temporal axis while holding for
the copy axis.

**Block deduplication and chunking.** Venti [6] established
content-addressed block storage; iDedup [5] studied inline deduplication
and its fragmentation cost; LBFS [8] introduced and FastCDC [9] optimised
content-defined chunking. Our shift experiment characterises whether CDC's
precondition (offset-drifted identical content) holds for checkpoints—it
does not—and our alignment control quantifies how badly fixed grids fare
without file boundaries.

**Learning in storage.** LinnOS [10] infers per-I/O flash latency with a
lightweight neural network and speculatively reroutes slow I/Os; KML [11]
runs learned models inside the kernel to set storage heuristics such as
readahead. In both, model output directly changes the I/O action. Our
asymmetric-loss gate differs structurally: ML decides only whether an exact
algorithm runs, never what data the algorithm returns.

**AI storage workloads.** Mooncake [12] characterised and accelerated
KVCache-centric serving traffic; checkpoint I/O studies and the FIU/MSR
block traces provide the traditional-workload baselines we defer to the
full-paper version. Serving KV caches are a separate, congested track we
deliberately do not enter.

## 7 Limitations and Conclusion

**Limitations.** One architecture (124 M GPT-2), one optimiser family
(AdamW), random-token training, CPU, and single-process axis fixtures:
absolute numbers will move at scale, but the mechanism (per-element updates
flip fp32 bytes; similarity keys are tensor names) is architecture- and
size-independent, and our axis controls isolate it directly. Field
multi-node traces, optimizer-state-only comparisons beyond our zero-block
check, non-AdamW optimisers, and a full CDC implementation (we provide
strong precondition evidence rather than an implementation) are clear
extensions. Read-side effects of deduplication fragmentation on
checkpoint restore are separate work.

**Conclusion.** Semantic checkpoint redundancy splits sharply at the POSIX
boundary: the byte layer sees every *copy* (rank replicas, shared bases:
100%) and nearly none of the *similarity* (steps, jobs, shards: ≈0%) that
semantic systems monetise—while scoring 59–98% on image layers and source
trees with the same code. Transparent storage cannot earn the 6–896×, and
should stop trying; it should learn to skip the workloads where bytes churn,
and reserve exact, correctness-preserving fingerprinting for the workloads
where copies live.

## References

1. W. Liu, S. Li, Z. Lai, K. Ge, Q. Chen, P. Sun, D. Li, K. Lu. AdaCheck: An Adaptive Checkpointing System for Efficient LLM Training with Redundancy Utilization. FAST 2026, pp. 271–289.
2. Y. Hao, T. Yao, X. Wei, D. Zhang, T. Sun, Y. Zhang, Z. Fu, H. Wu, R. Chen. Fast Cloud Storage for AI Jobs via Grouped I/O API with Transparent Read/Write Optimizations (system name: AITURBO). FAST 2026.
3. J. O'Connor, J.-P. Aumasson, S. Neves, Z. Wilcox-O'Hearn. BLAKE3: One Function, Fast Everywhere. Technical report, 2020. https://blake3.io
4. J. Kaiser, R. Gad, T. Süß, F. Padua, L. Nagel, A. Brinkmann. Deduplication Potential of HPC Applications' Checkpoints. IEEE CLUSTER 2016, pp. 413–422.
5. K. Srinivasan, T. Bisson, G. Goodson, K. Voruganti. iDedup: Latency-aware, Inline Data Deduplication for Primary Storage. FAST 2012.
6. S. Quinlan, S. Dorward. Venti: A New Approach to Archival Storage. FAST 2002.
7. Hugging Face. safetensors serialization format. https://huggingface.co/docs/safetensors (software; no archival paper).
8. A. Muthitacharoen, B. Chen, D. Mazieres. A Low-Bandwidth Network File System (LBFS). SOSP 2001.
9. W. Xia, Y. Zhou, H. Jiang, D. Feng, Y. Hua, Y. Hu, Q. Liu, Y. Zhang. The Design of Fast Content-Defined Chunking for Data Deduplication Based Storage Systems (FastCDC). IEEE TPDS 31(9):2017–2031, 2020.
10. M. Hao, L. Toksoz, N. Li, E. E. Halim, H. Hoffmann, H. S. Gunawi. LinnOS: Predictability on Unpredictable Flash Storage with a Light Neural Network. OSDI 2020.
11. I. U. Akgun, A. S. Aydin, A. Shaikh, L. Velikov, E. Zadok. A Machine Learning Framework to Improve Storage System Performance (KML). HotStorage 2021.
12. R. Qin, Z. Li, W. He, J. Cui, F. Ren, M. Zhang, Y. Wu, W. Zheng, X. Xu. Mooncake: Trading More Storage for Less Computation — A KVCache-centric Architecture for Serving LLM Chatbot. FAST 2025, pp. 155–170 (Best Paper).
