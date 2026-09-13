#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
gen_coinit.py — shared-init fleet fixture (redundancy axis B).

Job A of the main fixture (gen_checkpoints.py, seed 20260910) initialises
weights from torch.manual_seed(20260910) and streams tokens from a
generator seeded seed+1. This script runs job B: IDENTICAL initial weights
(same init seed) but a DIFFERENT data stream — the "many jobs fine-tuned
from one pretrained base" pattern. We save a weights-only snapshot of the
raw init plus weights-only checkpoints every step (zero-free bytes, no
optimizer padding), and one full torch.save at step 0.

Determinism: rebuilding the model twice under the same init seed must give
md5-identical state dicts (asserted), so B's init == A's init by
construction rather than by copying files.
"""

import argparse
import hashlib
import json
import os
import time

import torch
import torch.nn.functional as F

from gen_checkpoints import GPT2, gpt2_state_dict, tensor_bytes


def md5_state(sd):
    h = hashlib.md5()
    for k in sorted(sd):
        t = sd[k].detach().cpu().contiguous()
        h.update(k.encode())
        h.update(str(tuple(t.shape)).encode())
        h.update(t.numpy().tobytes())
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="../../output/checkpoint-dedup/coinit")
    ap.add_argument("--steps", type=int, default=6)
    ap.add_argument("--init-seed", type=int, default=20260910)
    ap.add_argument("--data-seed", type=int, default=20260912)
    ap.add_argument("--smoke", action="store_true")
    args = ap.parse_args()

    if args.smoke:
        n_layer, n_embd, n_head, steps = 2, 128, 4, 2
    else:
        n_layer, n_embd, n_head, steps = 12, 768, 12, args.steps

    torch.set_num_threads(max(1, os.cpu_count() or 1))

    # determinism check: same init seed twice -> identical weights
    torch.manual_seed(args.init_seed)
    model = GPT2(vocab_size=50257, n_positions=1024, n_embd=n_embd,
                 n_head=n_head, n_layer=n_layer)
    torch.manual_seed(args.init_seed)
    twin = GPT2(vocab_size=50257, n_positions=1024, n_embd=n_embd,
                n_head=n_head, n_layer=n_layer)
    md5_a, md5_b = md5_state(gpt2_state_dict(model)), md5_state(gpt2_state_dict(twin))
    assert md5_a == md5_b, "init not reproducible"
    print(f"init determinism OK, md5={md5_a}")

    optim = torch.optim.AdamW(model.parameters(), lr=1e-3, betas=(0.9, 0.999),
                              eps=1e-8, weight_decay=0.01)

    out_w = os.path.join(args.out, "weights")
    out_t = os.path.join(args.out, "torchsave")
    os.makedirs(out_w, exist_ok=True)
    os.makedirs(out_t, exist_ok=True)

    torch.save(gpt2_state_dict(model), os.path.join(out_w, "init.pt"))

    g = torch.Generator().manual_seed(args.data_seed)
    manifest = {
        "init_seed": args.init_seed, "data_seed": args.data_seed,
        "note": "job A = gen_checkpoints.py seed 20260910 (data 20260911)",
        "init_md5": md5_a,
        "steps": [],
    }
    for step in range(steps):
        t0 = time.time()
        idx = torch.randint(0, 50257, (1, 128), generator=g)
        logits = model(idx)
        loss = F.cross_entropy(logits[:, :-1].reshape(-1, 50257),
                               idx[:, 1:].reshape(-1))
        optim.zero_grad(set_to_none=True)
        loss.backward()
        optim.step()

        msd = gpt2_state_dict(model)
        p_w = os.path.join(out_w, f"step_{step:03d}.pt")
        torch.save(msd, p_w)
        if step == 0:
            torch.save({"model": msd, "optimizer": optim.state_dict(),
                        "step": step, "loss": float(loss)},
                       os.path.join(out_t, "step_000.pt"))
        manifest["steps"].append({"step": step, "loss": float(loss),
                                  "weights_bytes": os.path.getsize(p_w),
                                  "time_s": round(time.time() - t0, 2)})
        print(f"step {step:02d} loss={loss.item():.3f} "
              f"w={os.path.getsize(p_w)/1e6:.0f}MB")

    with open(os.path.join(args.out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print("manifest written:", os.path.join(args.out, "manifest.json"))


if __name__ == "__main__":
    main()
