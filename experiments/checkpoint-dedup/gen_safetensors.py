#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
gen_safetensors.py — R2: safetensors slot for the main 20-step fixture.

Replays the EXACT training sequence of gen_checkpoints.py (same seed,
RNG generator, model, optimizer, data order) but writes weights-only
snapshots in safetensors format (raw LE tensor payload + JSON header,
no zip container):  safetensors/step_XXX.safetensors

Purpose: close the "what about safetensors?" reviewer question with the
same chunk analyzer (file view; safetensors has no inner entries).
"""

import argparse
import os
import time

import torch
import torch.nn.functional as F
from safetensors.torch import save_file

from gen_checkpoints import GPT2, gpt2_state_dict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="../../output/checkpoint-dedup/safetensors")
    ap.add_argument("--steps", type=int, default=20)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    seed = 20260910
    torch.manual_seed(seed)
    torch.set_num_threads(max(1, os.cpu_count() or 1))
    model = GPT2(vocab_size=50257, n_positions=1024, n_embd=768,
                 n_head=12, n_layer=12)
    optim = torch.optim.AdamW(model.parameters(), lr=1e-3, betas=(0.9, 0.999),
                              eps=1e-8, weight_decay=0.01)
    g = torch.Generator().manual_seed(seed + 1)
    for step in range(args.steps):
        t0 = time.time()
        idx = torch.randint(0, 50257, (1, 128), generator=g)
        logits = model(idx)
        loss = F.cross_entropy(logits[:, :-1].reshape(-1, 50257),
                               idx[:, 1:].reshape(-1))
        optim.zero_grad(set_to_none=True)
        loss.backward()
        optim.step()
        p = os.path.join(args.out, f"step_{step:03d}.safetensors")
        save_file(gpt2_state_dict(model), p, metadata={"format": "pt"})
        print(f"step {step:02d} loss={loss.item():.3f} "
              f"{os.path.getsize(p)/1e6:.0f}MB train={time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
