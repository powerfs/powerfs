#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
gen_checkpoints.py — Phase-A fixture generator for the checkpoint-dedup paper.

Runs a real (CPU-only) GPT-2 training loop and snapshots checkpoints every
step in three on-disk layouts:

  torchsave/step_XXX.pt  — torch.save({"model", "optimizer", ...}) (new zip
                           container, what torch.load(full resume) consumes)
  weights/step_XXX.pt    — torch.save(model.state_dict()) only (weights-only
                           sharing / LoRA-style partial restore source)
  dcp/step_XXX/          — torch.distributed.checkpoint layout (__0_0.distcp
                           + .metadata), single-process single shard

The model is a self-contained GPT-2 (tied input/lm-head, pre-LN blocks,
Conv1D projections with the (in,out) weight layout used by HF transformers);
nothing is downloaded and the only runtime dependency is torch (+numpy).

Environment used for the paper measurements:
  python 3.8.10, torch 2.4.1+cpu, numpy 1.24.x, Linux x86_64 container,
  fixed seed (default 20260910) so the whole fixture is reproducible.

Usage:
  python3 gen_checkpoints.py --out ../../output/checkpoint-dedup
  python3 gen_checkpoints.py --smoke            # 2-layer tiny, 2 steps
"""

import argparse
import json
import math
import os
import time

import torch
import torch.nn as nn
import torch.nn.functional as F


# ---------------------------------------------------------------- GPT-2 core

class Conv1D(nn.Module):
    """HF-style GPT-2 projection: weight layout (nx, nf), y = x @ w + b."""

    def __init__(self, nx, nf):
        super().__init__()
        self.nf = nf
        self.weight = nn.Parameter(torch.empty(nx, nf))
        self.bias = nn.Parameter(torch.zeros(nf))
        nn.init.normal_(self.weight, std=0.02)

    def forward(self, x):
        size_out = x.size()[:-1] + (self.nf,)
        return torch.addmm(self.bias, x.view(-1, x.size(-1)), self.weight).view(size_out)


class CausalSelfAttention(nn.Module):
    def __init__(self, n_embd, n_head):
        super().__init__()
        assert n_embd % n_head == 0
        self.n_head = n_head
        self.n_embd = n_embd
        self.c_attn = Conv1D(n_embd, 3 * n_embd)
        self.c_proj = Conv1D(n_embd, n_embd)

    def forward(self, x):
        b, t, c = x.size()
        q, k, v = self.c_attn(x).split(self.n_embd, dim=2)
        hd = c // self.n_head
        q = q.view(b, t, self.n_head, hd).transpose(1, 2)
        k = k.view(b, t, self.n_head, hd).transpose(1, 2)
        v = v.view(b, t, self.n_head, hd).transpose(1, 2)
        y = F.scaled_dot_product_attention(q, k, v, is_causal=True)
        y = y.transpose(1, 2).contiguous().view(b, t, c)
        return self.c_proj(y)


class MLP(nn.Module):
    def __init__(self, n_embd):
        super().__init__()
        self.c_fc = Conv1D(n_embd, 4 * n_embd)
        self.c_proj = Conv1D(4 * n_embd, n_embd)

    def forward(self, x):
        return self.c_proj(F.gelu(self.c_fc(x), approximate="tanh"))


class Block(nn.Module):
    def __init__(self, n_embd, n_head, prefix):
        super().__init__()
        self.ln_1 = nn.LayerNorm(n_embd)
        self.attn = CausalSelfAttention(n_embd, n_head)
        # naming alias so state_dict keys match HF (transformer.h.N.attn.*)
        self.attn._load_prefix = prefix
        self.ln_2 = nn.LayerNorm(n_embd)
        self.mlp = MLP(n_embd)

    def forward(self, x):
        x = x + self.attn(self.ln_1(x))
        x = x + self.mlp(self.ln_2(x))
        return x


class GPT2(nn.Module):
    def __init__(self, vocab_size, n_positions, n_embd, n_head, n_layer):
        super().__init__()
        self.wte = nn.Embedding(vocab_size, n_embd)
        self.wpe = nn.Embedding(n_positions, n_embd)
        self.h = nn.ModuleList([Block(n_embd, n_head, str(i)) for i in range(n_layer)])
        self.ln_f = nn.LayerNorm(n_embd)
        for p in self.parameters():
            if p.dim() >= 2 and p.shape != self.wte.weight.shape:
                nn.init.normal_(p, mean=0.0, std=0.02)
        nn.init.normal_(self.wte.weight, std=0.02)
        nn.init.normal_(self.wpe.weight, std=0.01)

    def forward(self, idx):
        b, t = idx.size()
        pos = torch.arange(0, t, dtype=torch.long, device=idx.device)
        x = self.wte(idx) + self.wpe(pos)[None]
        for block in self.h:
            x = block(x)
        x = self.ln_f(x)
        return F.linear(x, self.wte.weight)  # tied lm-head


def gpt2_state_dict(model):
    """Return state dict with HF-aligned top-level keys (transformer.*)."""
    raw = model.state_dict()
    out = {}
    for k, v in raw.items():
        if k.startswith("h."):
            out["transformer." + k] = v
        elif k in ("wte.weight", "wpe.weight", "ln_f.weight", "ln_f.bias"):
            out["transformer." + k] = v
        else:
            out[k] = v
    return out


def tensor_bytes(obj):
    """Sum storage bytes of every tensor reachable in a nested structure."""
    total = 0
    if torch.is_tensor(obj):
        return obj.numel() * obj.element_size()
    if isinstance(obj, dict):
        for v in obj.values():
            total += tensor_bytes(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            total += tensor_bytes(v)
    return total


def dir_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="../../output/checkpoint-dedup")
    ap.add_argument("--steps", type=int, default=20)
    ap.add_argument("--seed", type=int, default=20260910)
    ap.add_argument("--seqlen", type=int, default=128)
    ap.add_argument("--batch", type=int, default=1)
    ap.add_argument("--smoke", action="store_true", help="tiny model, 2 steps")
    args = ap.parse_args()

    if args.smoke:
        n_layer, n_embd, n_head, steps = 2, 128, 4, 2
    else:
        # GPT-2 small 124M (vocab 50257, 1024 positions, 768 width, 12 heads)
        n_layer, n_embd, n_head, steps = 12, 768, 12, args.steps

    torch.manual_seed(args.seed)
    torch.set_num_threads(max(1, os.cpu_count() or 1))

    model = GPT2(vocab_size=50257, n_positions=1024, n_embd=n_embd,
                 n_head=n_head, n_layer=n_layer)
    n_params = sum(p.numel() for p in model.parameters())
    optim = torch.optim.AdamW(model.parameters(), lr=1e-3, betas=(0.9, 0.999),
                              eps=1e-8, weight_decay=0.01)

    import torch.distributed.checkpoint as dcp

    out_torch = os.path.join(args.out, "torchsave")
    out_weights = os.path.join(args.out, "weights")
    out_dcp = os.path.join(args.out, "dcp")
    for d in (out_torch, out_weights, out_dcp):
        os.makedirs(d, exist_ok=True)

    manifest = {
        "torch_version": torch.__version__,
        "config": {"vocab_size": 50257, "n_positions": 1024, "n_embd": n_embd,
                   "n_head": n_head, "n_layer": n_layer, "seqlen": args.seqlen,
                   "batch": args.batch, "seed": args.seed},
        "n_params": n_params,
        "optimizer": "AdamW lr=1e-3 betas=(0.9,0.999) eps=1e-8 wd=0.01",
        "steps": [],
    }
    print(f"model params: {n_params/1e6:.1f}M, steps={steps}")

    g = torch.Generator().manual_seed(args.seed + 1)
    for step in range(steps):
        t0 = time.time()
        idx = torch.randint(0, 50257, (args.batch, args.seqlen), generator=g)
        logits = model(idx)
        loss = F.cross_entropy(logits[:, :-1].reshape(-1, 50257), idx[:, 1:].reshape(-1))
        optim.zero_grad(set_to_none=True)
        loss.backward()
        optim.step()
        t_train = time.time() - t0

        msd = gpt2_state_dict(model)
        osd = optim.state_dict()

        t1 = time.time()
        p_full = os.path.join(out_torch, f"step_{step:03d}.pt")
        torch.save({"model": msd, "optimizer": osd, "step": step,
                    "loss": float(loss)}, p_full)
        t_full = time.time() - t1

        t2 = time.time()
        p_w = os.path.join(out_weights, f"step_{step:03d}.pt")
        torch.save(msd, p_w)
        t_w = time.time() - t2

        t3 = time.time()
        p_dcp = os.path.join(out_dcp, f"step_{step:03d}")
        os.makedirs(p_dcp, exist_ok=True)
        dcp.save(
            {"model": msd, "optimizer": osd},
            checkpoint_id=p_dcp,
        )
        t_dcp = time.time() - t3

        rec = {
            "step": step,
            "loss": float(loss),
            "model_tensor_bytes": tensor_bytes(msd),
            "optimizer_tensor_bytes": tensor_bytes(osd),
            "torchsave_bytes": os.path.getsize(p_full),
            "weights_bytes": os.path.getsize(p_w),
            "dcp_bytes": dir_size(p_dcp),
            "time_train_s": round(t_train, 2),
            "time_save_torch_s": round(t_full, 2),
            "time_save_weights_s": round(t_w, 2),
            "time_save_dcp_s": round(t_dcp, 2),
        }
        manifest["steps"].append(rec)
        print(f"step {step:02d} loss={loss.item():.3f} train={t_train:.1f}s "
              f"full={rec['torchsave_bytes']/1e6:.0f}MB w={rec['weights_bytes']/1e6:.0f}MB "
              f"dcp={rec['dcp_bytes']/1e6:.0f}MB save={t_full+t_w+t_dcp:.1f}s")

    with open(os.path.join(args.out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print("manifest written:", os.path.join(args.out, "manifest.json"))


if __name__ == "__main__":
    main()
