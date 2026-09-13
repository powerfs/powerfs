#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
gen_controls.py — robustness controls for the phase-A negative result.

Two obvious reviewer objections to "0% byte-identical chunks between
every-step checkpoints" are addressed:

  1. gap:        every-step is the *smallest* save interval; maybe long
                 intervals behave differently (they cannot create MORE
                 identical blocks, but we measure it anyway at 50/100 steps)
  2. precision: production checkpoints often store bf16 weights; bf16 has
                 only 7 mantissa bits, so tiny Adam updates may round to the
                 same representation and produce identical blocks

Saves (same GPT-2 small 124M model, same seed/training as gen_checkpoints):
  controls/weights_fp32/step_*.pt   at steps 0,1,2,9,10,49,50,98,99,100
  controls/weights_bf16/step_*.pt   same steps, tensors cast to bfloat16
  controls/torchsave/step_*.pt      full fp32 ckpt at steps 0,50,100 only
"""

import argparse
import json
import os
import time

import torch
import torch.nn.functional as F

from gen_checkpoints import GPT2, gpt2_state_dict, dir_size


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="../../output/checkpoint-dedup/controls")
    ap.add_argument("--seed", type=int, default=20260910)
    args = ap.parse_args()

    total_steps = 100
    weight_steps = [0, 1, 2, 9, 10, 49, 50, 98, 99, 100]
    full_steps = [0, 50, 100]

    torch.manual_seed(args.seed)
    torch.set_num_threads(max(1, os.cpu_count() or 1))
    model = GPT2(vocab_size=50257, n_positions=1024, n_embd=768, n_head=12, n_layer=12)
    optim = torch.optim.AdamW(model.parameters(), lr=1e-3, betas=(0.9, 0.999),
                              eps=1e-8, weight_decay=0.01)
    import torch.distributed.checkpoint as dcp  # noqa: F401 (parity with main run)

    dirs = {k: os.path.join(args.out, k)
            for k in ("weights_fp32", "weights_bf16", "torchsave")}
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)

    g = torch.Generator().manual_seed(args.seed + 1)
    manifest = {"steps": weight_steps, "full_steps": full_steps, "records": []}
    t_start = time.time()
    for step in range(total_steps + 1):
        if step > 0:
            idx = torch.randint(0, 50257, (1, 128), generator=g)
            logits = model(idx)
            loss = F.cross_entropy(logits[:, :-1].reshape(-1, 50257),
                                   idx[:, 1:].reshape(-1))
            optim.zero_grad(set_to_none=True)
            loss.backward()
            optim.step()
        if step not in weight_steps and step not in full_steps:
            continue
        msd = gpt2_state_dict(model)
        rec = {"step": step}
        if step in weight_steps:
            torch.save(msd, os.path.join(dirs["weights_fp32"], f"step_{step:03d}.pt"))
            bf16 = {k: v.to(torch.bfloat16) for k, v in msd.items()}
            torch.save(bf16, os.path.join(dirs["weights_bf16"], f"step_{step:03d}.pt"))
        if step in full_steps:
            torch.save({"model": msd, "optimizer": optim.state_dict(),
                        "step": step},
                       os.path.join(dirs["torchsave"], f"step_{step:03d}.pt"))
            rec["torchsave_bytes"] = os.path.getsize(
                os.path.join(dirs["torchsave"], f"step_{step:03d}.pt"))
        rec["fp32_bytes"] = os.path.getsize(
            os.path.join(dirs["weights_fp32"], f"step_{step:03d}.pt"))
        rec["bf16_bytes"] = os.path.getsize(
            os.path.join(dirs["weights_bf16"], f"step_{step:03d}.pt"))
        manifest["records"].append(rec)
        print(f"step {step:3d} saved  fp32={rec['fp32_bytes']/1e6:.0f}MB "
              f"bf16={rec['bf16_bytes']/1e6:.0f}MB  elapsed={time.time()-t_start:.0f}s")

    with open(os.path.join(args.out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)


if __name__ == "__main__":
    main()
