#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
gen_axes.py — R1 redundancy-axis fixture (paper F6 / Table 2).

The phase-A fixture only covers ONE redundancy axis: inter-step temporal
similarity within one training job. AdaCheck-style semantic dedup claims
6-896x over three *different* axes; this fixture materialises the other
two with the same self-contained GPT-2 small model:

  ddp_torch/rank{0..3}.pt    4 byte-identical full torch.save checkpoints
                             (DDP replicated-state save axis)
  ddp_st/rank{0..3}.*safetensors  4 identical safetensors replicas
  shard_torch/rank{0..3}.pt  FSDP/TP-style disjoint 1/4 parameter shards
  job_a/ vs job_b/           same architecture, independent seeds/data,
                             full .pt + weights .safetensors (inter-job axis)
  lora_job{1,2}/             identical shared base.safetensors + distinct
                             tiny adapter.pt (LoRA shared-base axis)

Two independent 3-step training runs (seed A=20260910, B=20260911).
"""

import argparse
import hashlib
import os
import shutil

import torch
import torch.nn.functional as F
from safetensors.torch import save_file

from gen_checkpoints import GPT2, gpt2_state_dict


def train_job(seed, steps=3, seqlen=128):
    torch.manual_seed(seed)
    torch.set_num_threads(max(1, os.cpu_count() or 1))
    model = GPT2(vocab_size=50257, n_positions=1024, n_embd=768,
                 n_head=12, n_layer=12)
    optim = torch.optim.AdamW(model.parameters(), lr=1e-3, betas=(0.9, 0.999),
                              eps=1e-8, weight_decay=0.01)
    g = torch.Generator().manual_seed(seed + 1)
    for step in range(steps):
        idx = torch.randint(0, 50257, (1, seqlen), generator=g)
        logits = model(idx)
        loss = F.cross_entropy(logits[:, :-1].reshape(-1, 50257),
                               idx[:, 1:].reshape(-1))
        optim.zero_grad(set_to_none=True)
        loss.backward()
        optim.step()
        print(f"  seed={seed} step {step} loss={loss.item():.3f}")
    return gpt2_state_dict(model), optim.state_dict()


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="../../output/checkpoint-dedup/axes")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    print("training job A ...")
    msd_a, osd_a = train_job(20260910)
    print("training job B ...")
    msd_b, osd_b = train_job(20260911)
    full_a = {"model": msd_a, "optimizer": osd_a, "step": 2}
    full_b = {"model": msd_b, "optimizer": osd_b, "step": 2}

    # --- axis 1: intra-job replication (DDP rank copies) ---
    d1 = os.path.join(args.out, "ddp_torch")
    os.makedirs(d1, exist_ok=True)
    p = os.path.join(d1, "rank0.pt")
    torch.save(full_a, p)                  # write once
    for r in range(1, 4):                  # byte copies, like replicated ranks
        shutil.copyfile(p, os.path.join(d1, f"rank{r}.pt"))

    d2 = os.path.join(args.out, "ddp_st")
    os.makedirs(d2, exist_ok=True)
    p = os.path.join(d2, "rank0.safetensors")
    save_file(msd_a, p, metadata={"format": "pt"})
    for r in range(1, 4):
        shutil.copyfile(p, os.path.join(d2, f"rank{r}.safetensors"))

    # --- axis 2: disjoint parameter shards (FSDP/TP) ---
    d3 = os.path.join(args.out, "shard_torch")
    os.makedirs(d3, exist_ok=True)
    keys = sorted(msd_a)
    for r in range(4):
        shard = {k: msd_a[k] for k in keys[r::4]}
        torch.save({"model": shard, "shard": r, "nshards": 4},
                   os.path.join(d3, f"rank{r}.pt"))

    # --- axis 3: inter-job, same architecture, different seed ---
    ja = os.path.join(args.out, "job_a")
    jb = os.path.join(args.out, "job_b")
    for d in (ja, jb):
        os.makedirs(d, exist_ok=True)
    torch.save(full_a, os.path.join(ja, "full.pt"))
    torch.save(full_b, os.path.join(jb, "full.pt"))
    save_file(msd_a, os.path.join(ja, "weights.safetensors"),
              metadata={"format": "pt"})
    save_file(msd_b, os.path.join(jb, "weights.safetensors"),
              metadata={"format": "pt"})

    # --- axis 4: LoRA-style jobs sharing one base, tiny distinct adapters ---
    base = os.path.join(args.out, "lora_base")
    os.makedirs(base, exist_ok=True)
    bp = os.path.join(base, "base.safetensors")
    save_file(msd_a, bp, metadata={"format": "pt"})
    for j, msd in ((1, msd_a), (2, msd_b)):
        dj = os.path.join(args.out, f"lora_job{j}")
        os.makedirs(dj, exist_ok=True)
        shutil.copyfile(bp, os.path.join(dj, "base.safetensors"))
        g = torch.Generator().manual_seed(900 + j)
        adapter = {f"lora.h{i}.A": torch.randn(8, 768, generator=g)
                   for i in range(12)}
        adapter.update({f"lora.h{i}.B": torch.randn(768, 8, generator=g)
                        for i in range(12)})
        save_file(adapter, os.path.join(dj, "adapter.safetensors"),
                  metadata={"format": "pt"})

    print("md5 ddp_torch replicas:",
          {f"rank{r}": md5_file(os.path.join(d1, f"rank{r}.pt"))[:8]
           for r in range(4)})
    for root, _dirs, files in os.walk(args.out):
        for f in sorted(files):
            p = os.path.join(root, f)
            print(f"{os.path.relpath(p, args.out):40s} "
                  f"{os.path.getsize(p)/1e6:8.1f} MB")


if __name__ == "__main__":
    main()
