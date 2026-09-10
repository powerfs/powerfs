# Phase-A results: cross-step chunk redundancy in GPT-2 small checkpoints

Hit ratio = chunk instances identical to some chunk in an earlier step (per fixed-size grid); steady-state mean over steps 5-19.

## 1. Cross-step hit ratio

| format | view | chunk | hit-any | hit-prev | intra-dup |
|---|---|---:|---:|---:|---:|
| dcp | file | 4K | 0.0037 | 0.0037 | 0.0037 |
| dcp | file | 64K | 0.0036 | 0.0036 | 0.0036 |
| dcp | file | 1M | 0.0028 | 0.0028 | 0.0021 |
| dcp | file | 4M | 0.0000 | 0.0000 | 0.0000 |
| torchsave | file | 4K | 0.0037 | 0.0037 | 0.0037 |
| torchsave | file | 64K | 0.0036 | 0.0036 | 0.0036 |
| torchsave | file | 1M | 0.0014 | 0.0014 | 0.0007 |
| torchsave | file | 4M | 0.0000 | 0.0000 | 0.0000 |
| torchsave | zipdata | 4K | 0.0037 | 0.0037 | 0.0037 |
| torchsave | zipdata | 64K | 0.0037 | 0.0037 | 0.0036 |
| torchsave | zipdata | 1M | 0.0029 | 0.0029 | 0.0022 |
| torchsave | zipdata | 4M | 0.0000 | 0.0000 | 0.0000 |
| weights | file | 4K | 0.0000 | 0.0000 | 0.0000 |
| weights | file | 64K | 0.0000 | 0.0000 | 0.0000 |
| weights | file | 1M | 0.0000 | 0.0000 | 0.0000 |
| weights | file | 4M | 0.0000 | 0.0000 | 0.0000 |
| weights | zipdata | 4K | 0.0000 | 0.0000 | 0.0000 |
| weights | zipdata | 64K | 0.0000 | 0.0000 | 0.0000 |
| weights | zipdata | 1M | 0.0000 | 0.0000 | 0.0000 |
| weights | zipdata | 4M | 0.0000 | 0.0000 | 0.0000 |

## 2. Offset-shift sensitivity (file view, adjacent steps)

hit ratio when the reference stream is shifted by N bytes (detects content that is identical but offset-drifting).

| format | chunk | shift 1 | 16 | 256 | 4096 |
|---|---:|---:|---:|---:|---:|
| dcp | 4K | 0.0037 | 0.0037 | 0.0037 | 0.0037 |
| dcp | 64K | 0.0036 | 0.0036 | 0.0036 | 0.0036 |
| dcp | 1M | 0.0028 | 0.0028 | 0.0028 | 0.0028 |
| dcp | 4M | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| torchsave | 4K | 0.0037 | 0.0037 | 0.0037 | 0.0037 |
| torchsave | 64K | 0.0036 | 0.0036 | 0.0036 | 0.0036 |
| torchsave | 1M | 0.0014 | 0.0014 | 0.0014 | 0.0014 |
| torchsave | 4M | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| weights | 4K | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| weights | 64K | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| weights | 1M | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| weights | 4M | 0.0000 | 0.0000 | 0.0000 | 0.0000 |

## 3. Controls: fp32 vs bf16, gap = 1/50/100 steps

| group | gap | chunk | hit ratio |
|---|---:|---:|---:|
| weights_fp32 | 1 | 4K | 0.000000 |
| weights_fp32 | 1 | 1024K | 0.000000 |
| weights_fp32 | 1 | 4K | 0.000000 |
| weights_fp32 | 1 | 1024K | 0.000000 |
| weights_fp32 | 1 | 4K | 0.000000 |
| weights_fp32 | 1 | 1024K | 0.000000 |
| weights_fp32 | 1 | 4K | 0.000000 |
| weights_fp32 | 1 | 1024K | 0.000000 |
| weights_fp32 | 1 | 4K | 0.000000 |
| weights_fp32 | 1 | 1024K | 0.000000 |
| weights_fp32 | 1 | 4K | 0.000000 |
| weights_fp32 | 1 | 1024K | 0.000000 |
| weights_fp32 | 50 | 4K | 0.000000 |
| weights_fp32 | 50 | 1024K | 0.000000 |
| weights_fp32 | 100 | 4K | 0.000000 |
| weights_fp32 | 100 | 1024K | 0.000000 |
| weights_fp32 | 50 | 4K | 0.000000 |
| weights_fp32 | 50 | 1024K | 0.000000 |
| weights_bf16 | 1 | 4K | 0.000115 |
| weights_bf16 | 1 | 1024K | 0.000000 |
| weights_bf16 | 1 | 4K | 0.000099 |
| weights_bf16 | 1 | 1024K | 0.000000 |
| weights_bf16 | 1 | 4K | 0.000132 |
| weights_bf16 | 1 | 1024K | 0.000000 |
| weights_bf16 | 1 | 4K | 0.000132 |
| weights_bf16 | 1 | 1024K | 0.000000 |
| weights_bf16 | 1 | 4K | 0.000099 |
| weights_bf16 | 1 | 1024K | 0.000000 |
| weights_bf16 | 1 | 4K | 0.000181 |
| weights_bf16 | 1 | 1024K | 0.000000 |
| weights_bf16 | 50 | 4K | 0.000000 |
| weights_bf16 | 50 | 1024K | 0.000000 |
| weights_bf16 | 100 | 4K | 0.000000 |
| weights_bf16 | 100 | 1024K | 0.000000 |
| weights_bf16 | 50 | 4K | 0.000000 |
| weights_bf16 | 50 | 1024K | 0.000000 |
| torchsave | 50 | 4K | 0.003688 |
| torchsave | 50 | 1024K | 0.000000 |
| torchsave | 100 | 4K | 0.003688 |
| torchsave | 100 | 1024K | 0.000000 |
| torchsave | 50 | 4K | 0.003688 |
| torchsave | 50 | 1024K | 0.002886 |

## 4. Adjacent-step similarity beyond exact chunks (zipdata)

| format | pair | exact 4K | zero 4K | bytes equal | zstd single | zstd XOR-delta |
|---|---|---:|---:|---:|---:|---:|
| weights | 0->1 | 0.0000 | 0.0000 | 0.236 | 1.081 | 1.186 |
| weights | 9->10 | 0.0000 | 0.0000 | 0.270 | 1.08 | 1.25 |
| weights | 18->19 | 0.0000 | 0.0000 | 0.294 | 1.08 | 1.278 |
| torchsave | 0->1 | 0.0037 | 0.0037 | 0.198 | 1.082 | 1.153 |
| torchsave | 9->10 | 0.0037 | 0.0037 | 0.263 | 1.085 | 1.223 |
| torchsave | 18->19 | 0.0037 | 0.0037 | 0.303 | 1.085 | 1.275 |

## 5. Go/no-go (decision rules)

- torchsave @1M file view, hit-any = 0.001404 (go >0.30, no-go <0.05)
- weights @1M file view, hit-any = 0.0 (go >0.30, no-go <0.05)
- dcp @1M file view, hit-any = 0.002809 (go >0.30, no-go <0.05)

