# 论文发表路线：热点结合方向与时间表

> 目的：在 [write-prediction-related-work.md](write-prediction-related-work.md) 的相关工作边界之上，结合 2026 年存储领域热点与 venue 时间窗口，确定可投方向、优先级与执行路线。
>
> 记录日期：2026-09-10。结论先行：**热点（AI 存储）与本项目资产（POSIX 透明内核客户端 + 学习型写门控 + RDMA）存在真实交叉，但必须先用极低成本的 go/no-go 测量验证 checkpoint 字节级冗余，再决定投入；KVCache 赛道明确不进。**

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

### 方向二（天花板高一档）：POSIX 透明 checkpoint 去重，打"通用性"缝隙

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
| 2026-09 中旬（本周） | **写侧 go/no-go**：容器内 CPU 装 PyTorch，GPT-2 small 连存 20 个 checkpoint（torch.save + DCP 两格式），离线切 1MB chunk 算跨 step 重复率与偏移敏感度 | 1–2 天出结果；同时决定方向二死活、构成方向一核心数据 |
| 2026-09 下旬 | **读侧最小验证**：复用同一批 checkpoint，全量加载 + 部分加载（model.* 子集）× readahead off/RULE:16/auto，cold cache 各 3 次；记录加载时延、读放大、volume IOPS | 确认"按加载意图自适应"分叉；量化 dedup 碎片对全量恢复的读侧代价 |
| 2026-10 ~ 11 | 补容器层 + FIU trace 测量；实现指纹 hit/miss 真实标签回流，根治策略死穴；评估碎片感知并行 RDMA 预读原型 | 规则 vs NN 在陌生 trace 上的 precision/recall/FPR；碎片预读原型数据 |
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

### 7.1 阶段 A：写侧 go/no-go（待测量）

- 夹具：模型配置 / 步数 / torch 版本：待测后填写。
- 关键结果：1MB 全同 chunk 跨 step 比例、偏移滑窗命中率、仅-weights 对比。
- 结论（go / no-go / CDC）：待定。
