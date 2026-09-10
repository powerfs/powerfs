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

## 5. Positive controls — where byte-level dedup DOES work

Same BLAKE2b-128 fixed-chunk measurement. `perfile` chunks every
file independently from offset 0 (the POSIX filesystem view);
`raw` chunks the archive byte stream (misaligned by tar ordering).
hit-any = fraction of chunks already present in ANY earlier version.

| group | view | version | 4K | 64K | 1M |
|---|---|---|---:|---:|---:|
| rootfs | perfile | rootfs_v2 | 0.5859 | 0.6232 | 0.6602 |
| rootfs | perfile | rootfs_v3 | 0.9775 | 0.9740 | 0.9810 |
| rootfs | raw | rootfs_v1 | 0.0000 | 0.0000 | 0.0000 |
| rootfs | raw | rootfs_v2 | 0.0393 | 0.0141 | 0.0000 |
| rootfs | raw | rootfs_v3 | 0.1036 | 0.0162 | 0.0000 |
| source | perfile | src_s2 | 0.3243 | 0.1148 | 0.0000 |
| source | perfile | src_s3 | 0.7046 | 0.5625 | 0.0000 |
| source | perfile | src_s4 | 0.7536 | 0.5846 | 0.0000 |
| source | perfile | src_s5 | 0.9477 | 0.9559 | 0.0000 |
| source | raw | src_s1 | 0.0000 | 0.0000 | 0.0000 |
| source | raw | src_s2 | 0.0142 | 0.0000 | 0.0000 |
| source | raw | src_s3 | 0.0455 | 0.0050 | 0.0000 |
| source | raw | src_s4 | 0.1169 | 0.0049 | 0.0000 |
| source | raw | src_s5 | 0.5431 | 0.0047 | 0.0000 |
| images | raw | v1 | 0.0000 | 0.0000 | 0.0000 |
| images | raw | v2 | 0.0033 | 0.0002 | 0.0000 |
| images | raw | v3 | 0.1908 | 0.0000 | 0.0000 |
| images | layers-perfile | v2 | 0.5860 | 0.6233 | 0.6602 |
| images | layers-perfile | v3 | 0.9775 | 0.9740 | 0.9810 |

OCI layer blobs (v3 image): 2; 1 shared across all 3 versions (75 of 272 MB, content-addressable, zero transfer/storage). Shared digests:
- `470b66ea5123c93b0d5…` in v1, v2, v3

## 6. Go/no-go (decision rules)

- torchsave @1M file view, hit-any = 0.001404 (go >0.30, no-go <0.05)
- weights @1M file view, hit-any = 0.0 (go >0.30, no-go <0.05)
- dcp @1M file view, hit-any = 0.002809 (go >0.30, no-go <0.05)

