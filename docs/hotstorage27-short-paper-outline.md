# HotStorage'27 短文 outline（锁定稿 2026-09-11）

> 目标 venue：HotStorage'27（约 2027 春末截稿，8 页 workshop，接受早期结果）；备选 PDSW'27（6 页，HPC 受众，引言改写）。
> 性质：**测量 + 立场**（measurement + position），不是系统论文；系统部分仅半页 existence proof。
> 数据来源：[roadmap 7.1/7.2](paper-publication-roadmap.md#7-测量结果记录按阶段追加) 与 `experiments/checkpoint-dedup/`（全部脚本可复现）。

## 1. 一句话立意

语义级 checkpoint 去重宣称的 6–896× 收益不是均质的：按**冗余来源轴**拆开后，POSIX 字节层能吃到的只有"整体复制"（跨 rank 副本、跨作业共享 base），而占收益大头的跨 step 时间相似与跨作业同架构相似在字节层 ≈0；这条边界可以在接触字节之前用便宜信号门控（learn-to-skip），预测错误只伤性能、不碰正确性。

## 2. 标题候选

1. *Where Byte-Level Checkpoint Deduplication Dies: A Boundary Study of Semantic Redundancy in AI Training Checkpoints*（首选）
2. *Invisible Savings: Why Semantic Checkpoint Dedup Does Not Reach the POSIX Byte Layer*
3. *To Hash or Not: A Redundancy Boundary across Checkpoints, Container Layers, and Source Archives*

## 3. 章节预算（正文约 6 页 + references）

### §1 Introduction（0.75p）

- checkpoint 容量随模型规模膨胀；FAST'26 AdaCheck/AITURBO 张量级语义去重报 6–896×，但均需改框架 / 专用 grouped-I/O API / XPU 卸载 hash。
- 问题：透明 POSIX 文件系统（unmodified PyTorch 挂载即受益）能否截获这些收益？
- 结果预告 Fig.1：同一把定长块尺子，checkpoint 跨 step 命中 0%，镜像层/源码 59–98%。
- 贡献三条：① 字节层对语义冗余可见性的首个对照测量；② 机制解释（排除 chunking/对齐/精度/间隔假设）；③ 对透明存储设计的含义（learn-to-skip 门控 + 非对称损失正确性论点）。

### §2 Background & Redundancy Taxonomy（1p）

- torch.save（zip+STORED pickle 分片）/ DCP / safetensors 格式速览；定长块字节去重模型与 hash CPU 成本（AITURBO 被迫 XPU 卸载的旁证）。
- **Table 1 冗余轴分类框架**（§4 填数据）：
  1. intra-job 副本轴：DP 全参数复制、TP embedding/优化器状态复制；
  2. inter-job 同构轴：同架构不同作业（LoRA base 共享、微调共享初始化）；
  3. inter-step 时序轴：逐步 checkpoint。
- 各轴在"无应用语义、仅 POSIX 可见"前提下的先验可观测信号。

### §3 Methodology（0.75p）

- 夹具：GPT-2 small 124M，真实 AdamW（lr=1e-3, wd=0.01），seqlen=128；主夹具 20 步、100 步对照（fp32/bf16、间隔 1/50/100）；torch 2.4.1+cpu，种子固定，全脚本开源可复现。
- 分析器：BLAKE2b-128，chunk ∈ {4K,64K,1M,4M}；视图 file / zipdata-entry / per-file / tar-raw；hit-any/hit-adj、全配对、shift 滑窗 {1,16,256,4096}；合成夹具自检（精确命中 20/40 等）。
- 正向对照：ubuntu:20.04 + apt 的 posctrl:v1/v2/v3（兄弟全量层，比真实增量链严苛）；powerfs 5 个历史 commit 的 git archive。
- 范围与威胁显式声明：单模型受控微基准，不冒充 field trace；机制结论与模型规模无关（逐元素更新），规模外推列入 limitations。

### §4 Findings（2p，核心）

- **F1 时序冗余不可见**：三格式 × 四档 0–0.37%，且命中全是零块（Fig.2）。
- **F2 对齐是必要非充分条件**：shift 滑窗零变化（无偏移漂移，fixed-grid/CDC 均无的可救）；同一批 workload 切 tar raw 流时镜像/源码也塌到 0.3–19%（边界对齐必要性）；checkpoint 的 zip entry 已对齐仍为 0（不是充分条件）。
- **F3 不是精度/间隔/压缩问题**：bf16 64K/1M 仍 0、50/100 步间隔仍 0；zstd 单文件 1.08×、XOR-delta ≤1.28×、逐字节相等率 20–30%。
- **F4 机制**：dense grad（tied embedding 反传产生非 None 零梯度）+ decoupled weight decay + Adam m/v 逐元素衰减 → fp32 每字节每步必变（Fig.4）。
- **F5 能力边界图**：镜像 59–98%、源码 32–95%（随版本距离衰减，Fig.3），OCI base 层内容寻址零传输；同方法同代码路径，排除"测量器坏了"。
- **F6 冗余轴地图（Table 2）**：跨 rank 整体复制高命中（透明层白赚）、shard 互不命中、跨作业同架构零命中（必须语义层）、LoRA 共享 base 整体文件命中——字节层能吃的是"复制轴"，吃不到"相似轴"。

### §5 Implications: learn-to-skip, not learn-to-dedup（0.75p，立场）

- 透明层正确动作：接触字节前分类负载——复制型 hash 必赚，时序浮点型 hash 必亏。
- 非对称损失：假阳性浪费一次指纹，假阴性损失一次节省；**预测错误只伤性能，精确指纹匹配是唯一抑制动作，ML 与正确性解耦**（区别于 KML/LinnOS）。
- 反向肯定语义系统：0% 量化了 AdaCheck/AITURBO 改框架的**必要性下限**——透明层不可能截胡其收益。
- PowerFS 内核去重路径 A/B（WriteNeedle RPC 1020→258、稳态带宽 +50%、MD5 一致）半页内作 existence proof，不冒充 checkpoint 结果。
- 真实 hit/miss 标签回流、FIU trace、门控器评测一句 future work 带过。

### §6 Related Work / Limitations / Conclusion（0.75p）

- AdaCheck / AITURBO / Kaiser / iDedup / KML / LinnOS 紧凑差异化。
- Limitations：单模型小规模；无真实多机训练 trace（用轴级合成夹具代替，机制论证）；CDC 未全实现（shift 证据 + 措辞克制）；读侧碎片不在本文。
- 结论一句：语义冗余在字节层按轴分裂；透明存储应学会跳过，而非学会去重。

## 4. 图表清单

| 图表 | 内容 | 数据状态 |
|---|---|---|
| Fig.1 | teaser：三类 workload 1M 命中对比（0% vs 66/98%） | 已有 |
| Fig.2 | chunk size × 格式命中率折线 | 已有 |
| Fig.3 | 命中率随版本/step 距离衰减三线图 | 已有 |
| Fig.4 | 逐字节相等率 + zstd/XOR-delta 压缩比 | 已有 |
| Table 1 | 冗余轴分类框架 | 写作 |
| Table 2 | 冗余轴 × workload 边界表 | R1 补齐 |

## 5. reviewer 必打点 × 防御

1. "DDP 多 rank 副本才是大头" → R1 的 F6 接住，把"无冗余"升级为"按轴分裂"。
2. "只测 124M、随机 token" → 机制与规模无关 + 显式 scope，引 Kaiser 现场特征化。
3. "safetensors 呢" → R2 同夹具同分析器。
4. "CDC 没实现" → shift 零变化强证据，全文措辞 fixed-grid/offset-drift，CDC 全称评估留 future。
5. "镜像对照人造" → 兄弟全量层比增量链严苛，反向措辞加分。

## 6. 明确不进短文

- 读侧 readahead 分叉 / dedup 碎片 scattered RDMA（独立卖点，留全文 EuroSys'28/FAST'28）；
- 四基线横向对比、崩溃注入、多客户端；
- FIU/MSR trace（全文阶段补）。

## 7. 执行顺序

1. R1 冗余轴夹具 + 分析（2026-09，~0.5–1 天）；
2. R2 safetensors 槽位（~0.5 天，含装包）；
3. 数据回填 roadmap 7.3、Table 2 定稿；
4. 2026-12 起按本 outline 成文，2027 春末前内部评审 → 投稿。
