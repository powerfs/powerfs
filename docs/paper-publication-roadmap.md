# 论文发表路线：热点结合方向与时间表

> 目的：在 [write-prediction-related-work.md](write-prediction-related-work.md) 的相关工作边界之上，结合 2026 年存储领域热点与 venue 时间窗口，确定可投方向、优先级与执行路线。
>
> 记录日期：2026-09-10；7.1/7.2/7.3 三组测量均于 09-10/11 完成。结论先行：**checkpoint 字节级去重按冗余轴分裂——复制轴（DDP 副本、LoRA 共享 base）命中 100%，相似轴（跨 step、同构异作业、分片间）≈0（压缩/CDC/bf16/长间隔/safetensors 全部对照失效）；同方法在镜像层/源码多版本上命中 59–98%。方向二否决，方向一定位为"语义级冗余为何在 POSIX 字节层不可见"的测量论文，outline 已锁定（[hotstorage27-short-paper-outline.md](hotstorage27-short-paper-outline.md)）。KVCache 赛道明确不进。**

## 1. Venue 时间窗口（2026-09 核实）

| Venue | 截稿 | 会期 | 适配度 | 备注 |
|---|---|---|---|---|
| FAST'27 fall | **2026-09-15（仅剩 5 天）** | 2027-02，Renton | 来不及 | 真实标签闭环、陌生 trace、多基线全缺，放弃，不浪费 idea 的首次评审 |
| OSDI'27 | abstract 2026-12-01 | 2027-07 | 3 个月赶全文不现实 | 不投 |
| HotStorage'27 | 约 2027 年春末（按惯例） | 2027 秋 | **首选** | 8 页 workshop，位置/早期结果即可，"容易发"的最佳窗口 |
| PDSW'27 | 约 2027-08 | 2027-11（SC 同期） | 备选 | 6 页 IEEE；HPC checkpoint 受众契合 |
| ATC'27 | 通常一季度（需届时核实） | 2027 夏 | 时间偏紧 | 视 go/no-go 后进展决定 |
| EuroSys'28 / FAST'28 | 2027 年春/秋两轮 | 2028 | **全文目标** | HotStorage 占坑后扩展 |
| 软件学报 / 计算机学报 | 滚动 | — | 保底 | 对"私有系统+完整实现+实验"接受度高；周期长、国际能见度低 |

佐证：FAST'27 CFP 已将 "Storage for AI and scientific workloads" 与 "AI-driven storage management and self-tuning" 同时列为正式 topic；StoreHub 2026 报告 FAST 的 AI 论文占比 2024 约 14% → 2025-26 超过 25%，连续两年 best paper 给 AI（Mooncake, FAST'25）。

## 2. 热点赛道拥挤度 × 资产匹配

| 热点方向 | 拥挤度 | 本项目匹配 | 判断 |
|---|---|---|---|
| KVCache tiering / prefix cache（Mooncake 之后：IMPRESS、CacheBlend、LMCache、SGLang、Marconi 等） | **红海** | 零（无 GPU、无 serving trace、无 KV 语义） | **明确不进** |
| LLM checkpoint 冗余利用（AdaCheck、AITURBO @ FAST'26） | 快速升温、尚未挤满 | **高**：内核 POSIX 透明客户端 + 写时门控 + RDMA；对手均需改框架/专用 API | **主攻** |
| 学习型存储自调优（FAST'27 官方 topic） | 中 | 中：有现成内核 NN 路径 | 作为方法论论点融入，不单独成文 |
| 容器镜像 / serverless 快照 | 中偏挤 | 中：透明客户端契合 | 作为第二 workload 家族 |

## 3. 候选方向

### 方向一（先走）：HotStorage'27 测量 + 立场短文

**卖点**：当不经修改的 AI 训练作业把 checkpoint 写进真实 POSIX 分布式文件系统时，字节级冗余长什么样——以及"非对称损失"的学习门控为何能在不牺牲正确性的前提下吃掉这些冗余。

HotStorage 8 页、接受早期结果，但**必须有一个让人意外的数据发现**。

- 复用资产：整套 PowerFS、内核 lockless dedup 热路径（[powerfs_addr.c](../kernel/powerfs_mod/powerfs_addr.c)）、WBTRACE、已有 A/B 数据（256MB×4 轮 WriteNeedle RPC 1020→258、稳态带宽 +50%、MD5 一致）。
- 最小实验集（3 个 workload）：
  1. 真实训练：CPU 跑 GPT-2 small 级别，连续 10–20 步每步存 checkpoint；`torch.save` 与 `torch.distributed.checkpoint` 两种格式各一组；
  2. 容器镜像层：`docker save` 若干共享 base layer 的镜像；
  3. FIU/MSR 传统 trace replay 作对照。
- 刻画指标：4KB/64KB/1MB/4MB 多档 chunk 的跨 step 重复率、重复距离、热 inode 集中度、未对齐重复比例。
- 立场贡献（核心新意）：ML 只决定"要不要算指纹"——假阳性浪费一次 hash，假阴性损失一次节省，**预测错误只伤性能**；任何抑制动作都以精确指纹匹配为前提，**ML 不确定性与存储正确性解耦**。这是区别于 KML/LinnOS 类预测式存储的论点。
- 反转价值：若测出"AdaCheck 报告的张量级 6–896× 冗余，在字节级透明视角下大部分消失"，负面发现本身划定透明路线适用边界，仍是好论文。

#### 读侧延伸：checkpoint 加载与碎片感知并行预读

checkpoint 生命周期包含写（save）与读（load）两侧，读侧按加载模式分三种，预读收益截然不同：

| 加载模式 | 读模式 | 预读收益 |
|---|---|---|
| 全量恢复（cold cache 从头读到尾，`torch.load` / safetensors 全量） | 单调递增顺序流 | **大**：预读流水线化掩盖 RDMA RTT，否则单流被 RTT 压到几十 MB/s |
| DCP 分片加载（每 rank 读各自 shard 文件） | 每客户端文件内顺序、多客户端并发 | 中等到大：单流预读有效，瓶颈转向 volume 端聚合带宽与调度 |
| 部分加载 / mmap 惰性（LoRA 只取 model weights、`safe_open` 按 key 取张量） | 稀疏跳跃读 | **有害**：RULE:16 类固定预读重演 47KB 场景 80× 放大教训（1.3 vs 105 MiB/s） |

两个机制层面的关键提醒：

1. **冷启动问题**：checkpoint 加载通常是"打开一次、读一遍、关闭"的一次性突发，现有 trace 聚合→异步训练→按 inode 下发 `NN:` 的闭环来不及（模型学出来加载已结束）；且写侧策略不能直接假设读侧也顺序（save/load 模式不对称）。读侧判定须依赖 open 模式（O_RDONLY）+ 文件大小 + **前几个 I/O 的 offset 单调性**快速决策，信心不足时保守回退（0.7 阈值）。
2. **去重碎片反噬（潜在独立论文点）**：写侧 dedup 使逻辑连续 chunk 映射到物理分散（甚至跨 volume）的 needle，compact 也不保证按逻辑读序摆放；全量恢复时逻辑顺序读在物理层变成 scattered RDMA，**单连接预读深度不够时，dedup 省下的写带宽会在读侧还回去**。iDedup (FAST'12) 讨论过碎片但未涉及 RDMA + 学习预读。可做机制：预读不只是"多读相邻 chunk"，而是对指纹索引已知的物理位置**并行下发多 chunk RDMA**，以请求并行度（而非字节放大）掩盖分散延迟。该机制基于确定性的碎片图，**独立于 NN 标签闭环成立**，可规避写侧头号死穴。

读侧最小验证（不改代码，VM/容器内真实 checkpoint，cold cache 各 3 次）：

1. GPT-2 small 存 20 个 step checkpoint，重启时 `torch.load` 全量加载；
2. 同一文件只加载 `model.*` 子集（模拟 LoRA 部分恢复）；
3. 三档对比：`readahead=off` / `RULE:16` / `auto`。

指标：加载墙钟时间、**读放大 = RDMA 实际传输字节 / 文件逻辑字节**、volume 端 IOPS。预期分叉：全量加载 auto≈16 > off；部分加载 off > auto ≫ RULE:16。分叉被实测确认，则"按加载意图自适应"论点成立，短文放一张 load time 对比图即有分量。

#### 方向一实施步骤（WBS）

实验脚本统一入库到 `experiments/checkpoint-dedup/`；原始数据与大文件输出到 `output/checkpoint-dedup/`（不入库），汇总指标 CSV 入库。

**阶段 A：写侧 go/no-go（2026-09 中旬，1–2 天，纯离线，不需要 PowerFS 集群）**

| 步骤 | 内容 | 产出 | 完成标准 |
|---|---|---|---|
| A0 环境 | 容器内准备 CPU 版 PyTorch（pip torch --index-url 官方 CPU wheel）；固定随机种子；记录 torch/python 版本 | 环境说明写入脚本头注释 | `import torch; torch.randn(1)` 可跑；无 GPU 依赖 |
| A1 夹具生成 | `gen_checkpoints.py`：GPT-2 架构（GPT2Config，随机初始化、**不下载权重**），真实 forward/backward/AdamW.step 跑 ≥20 步；每步保存两种格式：① `torch.save` 完整 ckpt（model+optimizer+step，zip 容器）；② `torch.distributed.checkpoint` 分片；另各存一组仅 model weights | `output/checkpoint-dedup/{torchsave,dcp,weights}/step_*.{zip,dir}`；manifest.json（每步字节数、tensor 列表与大小） | 20 步两种格式齐全；字节数随步变化符合预期（optimizer state 占大头） |
| A2 离线测量 | `analyze_chunks.py`：chunk size ∈ {4K,64K,1M,4M} 定长切分，BLAKE2b/SHA-256 指纹；计算：相邻 step 与全配对的全同 chunk 比例、重复距离分布、首次出现 step；偏移敏感度（相对上一步 ±{1,16,256,4096} 字节滑窗后命中率）；zip 内按 entry（data.pkl 各分片）分别统计；仅-weights 组对比 | `results/raw/*.csv` + 汇总 `results/summary.json` | 64+ 数据点（4 chunk 档 × 3 格式组 × 多配对）全部产出，可复跑（种子固定） |
| A3 决策 | 对照判据：**1MB 全同 chunk >30% → go**；<5% → no-go 放弃 checkpoint 故事；中间区看滑窗命中率，>50% → 需要 CDC，单列工程量后再决策 | 结论写入本文档第 7 节 | 给出 go/no-go/CDC 三选一明确结论与数据依据 |

**阶段 B：读侧最小验证（2026-09 下旬，复用 A1 同一批夹具）**

| 步骤 | 内容 | 完成标准 |
|---|---|---|
| B1 挂载 | 按硬约束在**容器内**装/用 powerfs-fuse（`/app/powerfs-fuse`，不用宿主机 fuse 跨连容器网络），挂载点放入 checkpoint 夹具 | 容器内挂载成功，md5 与本地夹具一致 |
| B2 全量加载 | cold cache（每次前清客户端缓存/重挂载）下 `torch.load` 全量恢复 × readahead `off`/`RULE:16`/`auto` 各 3 次 | 加载墙钟、RDMA 字节、volume IOPS 三指标 ×9 次记录 |
| B3 部分加载 | 同一 ckpt 只取 `model.*` 张量（模拟 LoRA 恢复），同样三档各 3 次 | 同三指标；验证预期分叉 off > auto ≫ RULE:16 |
| B4 碎片代价 | 对比 dedup=on/off 写入后全量加载的读时延与读放大 | 量化"写节省 vs 读代价"净值的第一组数据 |

**阶段 C：workload 扩展（2026-10～11）**

- C1 容器镜像层：取 ≥3 个共享 base layer 的镜像，`docker save` 为 tar，跑 A2 同一分析器；
- C2 FIU/MSR trace replay：trace 驱动重放器（仅测量用脚本，不改任何既有测试），同样多档 chunk 分析；
- C3 三类 workload 并排的重复率/距离/集中度对照表成文。

**阶段 D：真实标签闭环（2026-10～11，全文硬前提）**

- D1 指纹 hit/miss 结果回流为训练标签，替换规则伪造 label（[write_predict_policy.rs](../powerfs-filer/src/write_predict_policy.rs)）；
- D2 在 A/B/C 的 trace 上对比 always-hash、规则门控、NN 门控的 precision/recall/FPR 与实际节省。

**阶段 E：HotStorage'27 短文（2026-12～2027-03）**

- E1 大纲与卖点锁定（非对称损失 + 透明性测量 + 读侧分叉）；E2 实验图表；E3 8 页成文、内部评审、按截稿投稿。

### 方向二（~~天花板高一档~~ 已否决 2026-09-10）：POSIX 透明 checkpoint 去重

> **状态：NO-GO。** 阶段 A 实测全同 chunk ≈0、shift/CDC 无效、bf16 与长间隔对照一致、zstd/XOR-delta ≤1.28×，详见第 7.1 节。下面的论述仅保留作决策记录。


FAST'26 两篇的边界：**AdaCheck** 需张量级离线分析；**AITURBO** 需 grouped I/O 专用 API + XPU 卸载 BLAKE3 + job controller 集中判重；Kaiser (CLUSTER'16) 需改训练框架。共同隐含假设：应用愿意且能够配合。

本项目缝隙：**unmodified PyTorch / safetensors / 容器层，挂到 /mnt/powerfs 直接受益；随机覆写、部分写同样覆盖。** 借力论据：AITURBO 被迫把 hash 搬到 XPU，恰好证明 100Gbps 时代全量指纹 CPU 成本是真瓶颈；门控在接触内容字节之前决策，不需要 XPU。

- 路径：HotStorage 短文占坑 → 扩展全文冲 EuroSys'28 / FAST'28。
- 硬前提（缺一不可）：
  1. **go/no-go 测量先行**（即方向一实验 1，1–2 天）：相邻 step 的 1MB 全同 chunk >30% 值得做，<5% 放弃 checkpoint 故事；
  2. **真实标签闭环**：指纹 hit/miss 回流做标签，替换 [write_predict_policy.rs](../powerfs-filer/src/write_predict_policy.rs) 当前用规则（overwrite_ratio>0.5）伪造 label 的结构——头号死穴，任何 ML 全文绕不过；
  3. 至少两个 workload 家族（checkpoint + 容器层）证明通用性。
- 最大技术风险：内容相同但偏移漂移导致定长 chunk 全灭。测量后再决定是否上 content-defined chunking（独立工程量，不预先投入）。

### 方向三（保底）：中文 CCF-A 期刊

软件学报 / 计算机学报 / 计算机研究与发展。现有 A/B、compact 空间回收、崩溃正确性材料即可撑起系统类论文。用于毕业/职称保底，不作主线。

## 4. 明确不做

- **KVCache 任何变体**：血海，无 GPU 集群与真实 serving trace，零胜算。
- **赶 FAST'27 fall / OSDI'27**：证据链不全，仓促投只会用掉 idea 的首次评审机会。
- **纯 RDMA / needle compact / GC 论文**：工程扎实但无学术新颖性，预期审稿意见是"又一个 SeaweedFS 变体"。

## 5. 执行时间表

| 时间 | 事项 | 产出 / 决策点 |
|---|---|---|
| 2026-09 中旬（本周） | **写侧 go/no-go**：容器内 CPU 装 PyTorch，GPT-2 small 连存 20 个 checkpoint（torch.save + DCP 两格式），离线切 1MB chunk 算跨 step 重复率与偏移敏感度 | **已完成 2026-09-10：NO-GO（全同块 0-0.37%、shift/CDC/压缩/bf16/长间隔对照全部失效，见 7.1）**；方向二否决，方向一改为能力边界测量 |
| 2026-09 下旬 | **正向对照（已完成 2026-09-11，见 7.2）**：共享 base 的容器镜像 v1/v2/v3（docker commit/save/export）+ powerfs 5 个历史 commit 的 git archive，同一分析器 per-file/raw/layer 三视图 | 边界图成立：镜像 59–98%、源码 32–96%（近版本）、checkpoint ≈0；附边界对齐发现（tar raw 流塌到 0.3–19%） |
| 2026-09 下旬 | **读侧最小验证**：复用同一批 checkpoint，全量加载 + 部分加载（model.* 子集）× readahead off/RULE:16/auto，cold cache 各 3 次；记录加载时延、读放大、volume IOPS | 确认"按加载意图自适应"分叉（独立于去重结论） |
| 2026-10 ~ 11 | FIU trace 测量；实现指纹 hit/miss 真实标签回流（checkpoint 作为负样本），根治策略死穴 | 规则 vs NN 在陌生 trace 上的 precision/recall/FPR |
| 2026-12 ~ 2027-03 | 撰写 HotStorage'27 短文（8 页） | 投稿 |
| 2027 春之后 | 补四基线（always-hash 客户端、服务端去重/SeaweedFS、规则门控、NN 门控）、lease/refcount 崩溃注入与多客户端测试、KML 标准的内核推理开销预算 | 扩展全文 → EuroSys'28 / FAST'28 |

## 6. 待补实验缺口（全文阶段，备忘）

- FIU/MSR trace、容器镜像层、AI checkpoint trace（引 Kaiser CLUSTER'16）；
- 四基线横向对比；
- lease/refcount 崩溃注入与多客户端并发正确性；
- 内核推理开销预算对标 KML（<0.2% CPU 量级的论证标准）；
- fio/io500 标准基准数据（不得用自造脚本替代）；
- 读侧：checkpoint 三种加载模式（全量 / DCP 分片 / 部分加载）的读放大与时延；一次性突发读的预读冷启动决策；dedup/compact 后物理碎片度与 scattered RDMA 预取深度敏感性；多 rank 并发恢复的 volume 聚合带宽；
- 写读联合视角：同一 checkpoint 工作集上"写节省 vs 读代价"的净值，避免只报写侧收益。

## 7. 测量结果记录（按阶段追加）

### 7.1 阶段 A：写侧 go/no-go —— 已完成（2026-09-10），结论：**对 checkpoint 字节级去重 no-go**

**夹具**：GPT-2 small 124M（50257 vocab / 768 / 12 头 / 12 层，tied head），真实 AdamW（lr=1e-3, betas=(0.9,0.999), wd=0.01），seqlen=128×1×20 步；torch 2.4.1+cpu / py3.8；种子 20260910；torchsave 1493MB、weights 498MB、dcp 1494MB 每步（共 65GB）。脚本与原始结果：[experiments/checkpoint-dedup/](../experiments/checkpoint-dedup/)（`results_report.md` 可由脚本完整复现）。

**核心数据（稳态 step5-19 均值，跨 step 全同 chunk 比例）**：

| 格式 / 视角 | 4K | 64K | 1M | 4M |
|---|---:|---:|---:|---:|
| torchsave / file | 0.0037 | 0.0036 | **0.0014** | 0 |
| torchsave / zipdata | 0.0037 | 0.0037 | 0.0029 | 0 |
| weights / file | 0 | 0 | **0** | 0 |
| dcp / file | 0.0037 | 0.0036 | 0.0028 | 0 |

- 那 0.37% 的 4K 命中**全部是全零块**（`exact_4k == zero_4k`），不是内容重复；
- **偏移敏感度为零**：shift ∈ {1,16,256,4096} 命中率不升反与对齐值相同 → 不存在"内容相同、起点漂移"，**CDC 也救不了**；
- **鲁棒性对照**（`controls_overlap.csv`，同一训练跑 100 步）：fp32 weights 在间隔 1/50/100 步、所有档全为 **0**；bf16 weights 4K 仅 0.01%（60726 块中 6-11 块），64K/1M 为 0；间隔拉大不产生冗余；
- **通用压缩也失效**（`delta_compression.csv`，zipdata 相邻步）：单文件 zstd-1 仅 1.08×；逐字节相同率仅 20-30%；XOR-delta 流 zstd-1 最高仅 1.28×（fp32 低位噪声）。

**机制解释**：① tied lm-head 的反向给整个 embedding 矩阵 dense 梯度；embedding 反向对未采样行产生零（非 None）梯度，AdamW 的 decoupled weight_decay 仍逐元素衰减；② Adam m/v 每步对全参数衰减更新。→ 每个 fp32 字节每步都变，张量级语义冗余（AdaCheck 的并行/架构/迭代间相同张量、AITURBO 的 grouped I/O）在定长字节块视角不可见。

**对路线的影响**：

1. **方向二（POSIX 透明 checkpoint 去重）否决**：物理机制决定字节层无冗余可吃，继续投入是逆结论而行。
2. **方向一反而更强**：论文卖点从"透明去重有效"改为**能力边界的实证划分**——"Why semantic checkpoint dedup (AdaCheck/AITURBO: 6-896×) is invisible at the POSIX byte layer: a measurement study"。HotStorage 风格的负面测量 + 立场，且非对称损失门控论点在 checkpoint 场景的正确行为是**学会不 hash**（NN 门控需要 checkpoint 作为负样本，反哺真实标签闭环）。
3. **必须补正向对照**（下一步，原 C1 提前）：字节级去重在哪有效——容器镜像层 / 源码 tar 多版本 / 日志，形成完整边界图后短文证据链才闭环。
4. 读侧实验（阶段 B）价值下降但不取消：checkpoint load 的 readahead 分叉仍独立成立（与去重无关）。

### 7.2 正向对照：字节级去重的有效区间 —— 已完成（2026-09-11）

为避免"测量方法失效"的 reviewer 质疑，用同一套 BLAKE2b-128 定长块机械测两个**应当有效**的 workload（脚本 `analyze_archives.py`，原始数据 `positive_overlap.csv` / `positive_layers.csv`）：

- **容器镜像**：ubuntu:20.04 上用 apt 装包构造 posctrl:v1/v2/v3（147/260/266MB；三个应用层是同一 base 的兄弟全量层，非增量链，比增量链更严苛的对照）；
- **源码多版本**：powerfs 仓库 5 个跨度递增的历史 commit（s1 最早 → s5=HEAD，间隔约 276/60/60/20 commits）的 `git archive` tar。

**关键数字（文件系统视图：每个文件从偏移 0 独立切块，跨历史所有版本 hit-any）**：

| workload | 4K | 64K | 1M |
|---|---:|---:|---:|
| 镜像 rootfs v2（vs v1） | 0.586 | 0.623 | 0.660 |
| 镜像 rootfs v3（vs v1+v2） | **0.978** | 0.974 | **0.981** |
| 源码 s2（跨 ~276 commits） | 0.324 | 0.115 | — |
| 源码 s3 / s4 | 0.705 / 0.754 | 0.563 / 0.585 | — |
| 源码 s5（相邻版本） | **0.948** | **0.956** | — |
| checkpoint（7.1 对照） | **0**（bf16 0.0001） | 0 | 0 |

- OCI 内容寻址层：v3 的 272MB 中 base 层 75MB 与 v1/v2 digest 完全相同（零传输/存储）；即使三个应用层各自全量打包，191MB v2 层 59% 块、197MB v3 层 98% 块已在历史中（dpkg 重装/升级覆盖同内容文件）；
- **边界对齐的必要性（附带发现）**：若不按文件边界、直接对 tar 字节流切块，镜像/源码命中率也塌到 0.3–19%（tar 成员顺序与头错位）；checkpoint 的 zipdata 视图已按 entry 边界对齐却仍为 0 → 证明 checkpoint 失败不是边界问题，而是**边界内的 fp32 训练状态版本间不具字节稳定性**。

**边界图结论（短文核心表）**：字节级透明去重在"版本间保持字节稳定的文件单元"（系统二进制、库、源码、镜像层）上命中 59–98%；在"每步全参数浮点更新"的训练 checkpoint 上 ≈0。NN 门控的价值正是在接触字节前区分这两类工作负载。

### 7.3 R1/R2：冗余轴地图与 safetensors 槽位 —— 已完成（2026-09-11）

回应 outline 定稿后识别的两个 reviewer 必打点（脚本 `gen_axes.py`/`analyze_axes.py`/`gen_safetensors.py`/`analyze_safetensors.py`，数据 `axes_overlap.csv`、`safetensors_metrics.csv`）。

**R1 冗余轴（F6 / Table 2，同一 GPT-2 small 训练状态，per-file 对齐）**：

| 轴 | 对比 | 4K | 64K | 1M |
|---|---|---:|---:|---:|
| intra-job 复制（DDP rank 副本，torch.save） | rank r vs 早期 ranks | **1.0000** | **1.0000** | **1.0000** |
| intra-job 复制（safetensors 副本） | 同上 | **1.0000** | **1.0000** | **1.0000** |
| FSDP/TP 不相交参数分片 | rank r vs 早期 ranks | 0 | 0 | 0 |
| inter-job 同构异种子（完整 ckpt） | job B vs A | 0.0000 | 0.0000 | 0.0007（1/1423，噪声） |
| inter-job 同构异种子（weights safetensors） | job B vs A | 0 | 0 | 0 |
| LoRA 共享 base 文件 | job2 vs job1 base | **1.0000** | **1.0000** | **1.0000** |
| LoRA 独立 adapter | job2 vs job1 adapter | 0 | 0 | — |
| LoRA 作业目录整体 | 目录对目录 | 0.9988 | 0.9988 | 1.0000 |

- DDP 四副本 md5 完全相同（`a139b0b9…` torch、`c39187fe…` safetensors），zipdata 视图同为 1.0。
- **F6 结论**：字节层吃得到的是**复制轴**（整体文件/张量副本：DP 副本、LoRA base 共享），吃不到**相似轴**（逐步时序 7.1、同构异作业、分片间）；AdaCheck/AITURBO 收益的大头在相似轴，这量化了"为什么必须语义化"的下限，也说明透明层在 DDP 多副本保存场景仍有合法收益（门控应放行）。

**R2 safetensors**：与主夹具**完全相同的 20 步训练序列**（前 5 步 loss 逐位一致 10.989/10.916/10.968/11.021/10.935），仅换容器格式。稳态跨 step 命中：4K 0.000025（极少量巧合块），64K/1M/4M 全 **0**——结论与 zip 格式无关。
