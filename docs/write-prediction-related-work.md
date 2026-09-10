# 写预测去重：相关工作差异矩阵（Related Work Survey）

> 目的：为「按 inode 在线学习的写冗余预测，在客户端内核 writeback 热路径门控整条内容流水线（hash → 指纹 RPC → RDMA 发送 → 落盘）」这一候选论文点，建立相关工作边界。
>
> 调研日期：2026-09。结论先行：**精确组合缝隙存在，但很窄**；成败主要取决于真实标签闭环与真实 trace 实验，而非机制实现。
>
> 配套文档：发表方向、热点分析与执行时间表见 [paper-publication-roadmap.md](paper-publication-roadmap.md)。

## 0. 本项目机制的精确定位

- **决策点**：客户端内核 writeback 热路径（[powerfs_addr.c](../kernel/powerfs_mod/powerfs_addr.c)），数据离开客户端之前。
- **门控信号**：filer 端 [write_predict_policy.rs](../powerfs-filer/src/write_predict_policy.rs) 按 inode 的写行为特征（覆写率、可去重率、同步频率等 7 特征）在线学习 P(重复写)，通过 xattr `NN:<threshold>` 下发策略；**不接触内容字节**。
- **双层结构（关键）**：学习模型只门控「是否值得做内容相关操作」；真正的写抑制仍以**精确指纹（SHA-256）匹配**为前提。
- **被门控的整条流水线**：内容 hash（CPU）→ FingerprintLookup RPC（元数据 RTT）→ RDMA 数据发送（网络字节 + MR）→ 服务端落盘（容量）。
- **接入方式**：POSIX 透明、内核态，覆盖随机覆写 / checkpoint / 日志等非批量场景。

## 1. 去重系统谱系

| 系统 | 决策点（在哪省） | 选择性门控信号 | 是否对所有块算 hash | 实际省什么 | 语义 / 接入 |
|---|---|---|---|---|---|
| **LBFS** (SOSP'01) | 客户端用户态，close-to-open 批处理 | 无 | 是（CDC + SHA-1 全算） | 网络字节（GETHASH/CONDWRITE，服务端 NOTFOUND 才传） | NFS 衍生协议，非 POSIX 透明 |
| **DDFS / Data Domain** (FAST'08) | 服务端 inline | 无（Bloom filter 加速索引） | 是 | 存储 + 磁盘索引 IO | 备份 appliance |
| **Sparse Indexing** (FAST'09) | 服务端 | 采样稀疏索引，只查相似 segment（采样的是索引查询，非 hash） | 是 | 索引 RAM / 磁盘寻道 | 备份流 |
| **iDedup** (FAST'12) | 服务端主存 inline | 空间局部性：连续重复块序列达阈值才去重；时间局部性 → 内存指纹缓存 | 是 | 存储，并控制碎片 | WAFL 内部 |
| **DeDe** (ATC'09) | 集群 FS 各主机（VMware VMFS） | 无，主机写摘要日志、后台周期回收 | 是 | 存储（SAN，无网络可省） | 离线、VM 镜像 |
| **HPDedup** (2017) | 云主存服务端 | 按流估计的时间/空间局部性，动态分配指纹缓存与阈值 | 是（门控缓存准入，非 hash） | 存储 | 多租户 VM |
| **DD BOOST** (MSST'17) | **客户端库**，备份时 | 无 | 是 | **网络流量降 90–99%**（先发全量指纹，服务端判重） | 备份库 API，2010 起商用 |
| **Windows Server 2012 主存去重** (ATC'12) | 服务端 post-process | 分区策略 | 是 | 存储 | 企业文件服务 |
| **PowerFS（本项目）** | **客户端内核 writeback 热路径** | **按 inode 写行为特征在线学习，在任何内容相关操作之前门控**；命中仍需精确指纹匹配 | **否——非重复 inode 连 SHA-256 都不算、指纹 RPC 都不发** | hash CPU + 元数据 RPC + RDMA 字节 + fsync RTT + 容量，五项同省 | POSIX 透明、内核态 |

## 2. 网络冗余消除（RE）与内核 ML

| 系统 | 核心机制 | 与本项目重叠 | 关键差异 |
|---|---|---|---|
| **Spring–Wetherall** (SIGCOMM'00) | 协议无关字节流 RE | 「识别重复字节不发」 | 中间盒、无文件语义、对 TLS/RDMA 失效 |
| **EndRE** (NSDI'10) | 端侧 RE | RE 搬到端系统 | 服务端编码，按客户端缓存；字节流；全流量无门控 |
| **PACK** (SIGCOMM CCR'11) | 端侧**预测式** TRE，用已收 chunk 链预测后续 chunk | 名字即 "power of prediction"，必被对比 | 预测**字节流相关性**（统计匹配）而非文件写行为；对所有 TCP 流量透明、无选择性；无存储引用/GC 语义 |
| **SmartRE** (SIGCOMM'09) | 网络级协调 RE | 选择性缓存分配思想 | 中间盒资源协调，无文件/无学习 |
| **SDRE** (GLOBECOM'12) | 按内容类型选择性 RE | 「廉价信号决定对谁去重」 | 静态 content-type 标签；字节流；无学习、无文件粒度 |
| **KML** (HotStorage'21; ACM TOS'23) | 内核内 ML 替换存储启发式（readahead、NFS rsize），<0.2% CPU | 「内核里跑 NN」不新；方法论标尺 | 只读侧调参；模型纯内核；无写抑制、无正确性敏感引用协议；评测含未见混合负载，我们须达到同等严谨度 |
| **LinnOS** (OSDI'20) | 每 I/O NN 推断 SSD 延迟，4–6µs/IO，87–97% 准确 | 内核每 I/O 轻量推理的开销与误判对冲 | 预测设备延迟做 hedging；不涉及数据归并/引用 |
| **Leap** (ATC'20) | 内核 + RDMA 远内存多数表决预取 | 同属内核 RDMA 数据路径 | 读预取，非写去重 |
| **AITURBO** (FAST'26) | AI checkpoint 的 BLAKE3 客户端去重 + grouped I/O | **目标场景（checkpoint）正面相撞，最新顶会** | 应用专用 API、XPU 算 hash、job controller 集中判重；不透明、非 POSIX、无在线写行为模型 |
| Kaiser et al. (CLUSTER'16) | HPC checkpoint 去重潜力实测 | 证明目标负载真实存在 | trace 研究，可作为 workload 论证引用 |

## 3. 三个必须正面处理的「致命碰撞」

### 3.1 DD BOOST / LBFS：客户端先发指纹省网络（2001/2010 已有）

「客户端去重省带宽」**不能**作为贡献声明。可辩护差异三层：

1. 它们每个 chunk 都算 hash、每轮全量指纹发服务端判重（DD BOOST 流量降一个数量级，但元数据 RPC 照付）；我们在**指纹计算之前**门控，热 inode 重复轮次元数据 RPC = 0（实测 256MB×4 轮：WriteNeedle RPC 1020 → 258，r2–r4 为 0）。
2. 它们是备份/同步库 API、整文件批处理、close-to-open 语义；我们 POSIX 透明、内核 writeback，覆盖随机覆写等非批量场景。
3. 判重权威：它们在服务端、每次询问；我们引入本地抑制缓存 + 引用/lease 协议——DD BOOST 不需要而我们必须解决的正确性问题，是**潜在贡献点**。

### 3.2 iDedup / HPDedup / Sparse Indexing：选择性去重早已有之

精确分界线：先前工作门控的是**索引查询 / 缓存准入 / 碎片代价**，hash 照样对每块计算；我们门控**整条内容相关流水线**，信号是**不接触内容字节的写行为特征**——不是物理布局（iDedup 序列长度）、不是采样（Sparse Indexing）、不是静态标签（SDRE）、也不是历史局部性统计（HPDedup）。

### 3.3 KML + PACK：内核 ML 与「预测式 RE」都不新

独有的组合是**学习门控 + 精确内容验证的双层结构**。可写入 intro 的论点：

> 学习模型的预测错误在结构上不可能导致数据损坏：假阳性（预测重复但实际不重复）只浪费一次后台 hash/查询，假阴性只损失一次节省；真正的抑制动作始终以精确指纹匹配为前提。这种**非对称损失结构**把 ML 的不确定性与存储正确性完全解耦。

## 4. 贡献声明白名单 / 黑名单

**不能声明**：

- 首个客户端去重（LBFS / DD BOOST）
- 首个选择性去重（iDedup / HPDedup）
- 首个内核存储 ML（KML / LinnOS）
- 首个预测式冗余消除（PACK）
- checkpoint 去重（AITURBO）

**补齐实验后可以声明**：

1. 首个在**内核 writeback 层、任何内容相关操作之前**，以**按文件在线学习的写冗余预测**门控整条写流水线的透明 POSIX 机制；
2. 学习门控的**非对称损失结构**与误判安全边界（性能错误与正确性错误解耦）；
3. 本地抑制缓存在 **append-only + GC/compact 存储 + 多客户端**下的引用计数 / lease / close 时重指协议，及崩溃正确性论证；
4. RDMA 数据中心环境下端到端证据：网络字节、hash CPU、尾延迟、容量四维收益。

## 5. 论文成立的硬门槛

| 审稿人必问 | 现状 | 必须补的证据 |
|---|---|---|
| NN 打得过规则吗？ | 训练标签由规则伪造，特征与规则同源——**当前必然打不过，头号死穴** | 指纹 hit/miss 真实标签闭环；规则 vs NN 在陌生 trace 上 precision/recall/FPR |
| 真实负载命中率？ | 仅 fio 合成 + 最佳情况玩具负载 | FIU / MSR trace、容器镜像层、AI checkpoint（引 Kaiser'16 的分布论证），报告命中率分布而非平均值 |
| 相对「全指纹客户端基线」赢多少？ | 无 | always-hash-client、server-side dedup（vendor 的 SeaweedFS）、规则门控、NN 门控四基线 |
| lease/refcount 在崩溃和 compact 下安全？ | 有机制无论证 | 形式化不变量 + 崩溃注入 + 多客户端并发测试 |
| 内核推理开销？ | 每 inode 一次（xattr 下发），非每 chunk | 按 KML 标准报 CPU/内存预算；热路径 lockless 查询延迟 |
| 与 AITURBO (FAST'26) 何异？ | 未引用 | 透明 POSIX vs 专用 API；行为模型 vs 全量 XPU hash |

## 6. 参考文献（BibTeX）

```bibtex
@inproceedings{lbfs01,
  author = {Muthitacharoen, Athicha and Chen, Benjie and Mazi\`eres, David},
  title = {A Low-bandwidth Network File System},
  booktitle = {Proc. ACM SOSP}, year = {2001}
}

@inproceedings{ddfs08,
  author = {Zhu, Benjamin and Li, Kai and Patterson, Hugo},
  title = {Avoiding the Disk Bottleneck in the {Data Domain} Deduplication File System},
  booktitle = {Proc. USENIX FAST}, year = {2008}
}

@inproceedings{sparse09,
  author = {Lillibridge, Mark and Eshghi, Kave and Bhagwat, Deepavali and
            Deolalikar, Vinay and Trezise, Greg and Camble, Peter},
  title = {Sparse Indexing: Large Scale, Inline Deduplication Using Sampling and Locality},
  booktitle = {Proc. USENIX FAST}, year = {2009}
}

@inproceedings{idedup12,
  author = {Srinivasan, Kiran and Bisson, Tim and Goodson, Garth and Voruganti, Kaladhar},
  title = {{iDedup}: Latency-aware, Inline Data Deduplication for Primary Storage},
  booktitle = {Proc. USENIX FAST}, year = {2012}
}

@inproceedings{dede09,
  author = {Clements, Austin T. and Ahmad, Irfan and Vilayannur, Murali and Li, Jinyuan},
  title = {Decentralized Deduplication in {SAN} Cluster File Systems},
  booktitle = {Proc. USENIX ATC}, year = {2009}
}

@article{hpdedup17,
  author = {Wu, Huijun and Wang, Chen and Fu, Yinjin and Sakr, Sherif and
            Zhu, Liming and Lu, Kai},
  title = {{HPDedup}: A Hybrid Prioritized Data Deduplication Mechanism for Primary Storage in the Cloud},
  journal = {arXiv:1702.08153}, year = {2017}
}

@inproceedings{ddboost17,
  author = {Douglas, Fred and Huber, Andrew and Lewis, Donna and Traylor, Rachel},
  title = {Experiences with a Distributed Deduplication API},
  booktitle = {Proc. MSST}, year = {2017}
}

@inproceedings{msdedup12,
  author = {El-Shimi, Ahmed and Kalach, Ran and Kumar, Ankit and Oltean, Adi and
            Li, Jin and Sengupta, Sudipta},
  title = {Primary Data Deduplication -- Large Scale Study and System Design},
  booktitle = {Proc. USENIX ATC}, year = {2012}
}

@inproceedings{spring00,
  author = {Spring, Neil T. and Wetherall, David},
  title = {A Protocol-Independent Technique for Eliminating Redundant Network Traffic},
  booktitle = {Proc. ACM SIGCOMM}, year = {2000}
}

@inproceedings{endre10,
  author = {Aggarwal, Bhavish and Akella, Aditya and Anand, Ashok and
            Balachandran, Athula and Chitnis, Pushkar and Muthukrishnan, Chitra and
            Ramjee, Ramachandran and Varghese, George},
  title = {{EndRE}: An End-System Redundancy Elimination Service for Enterprises},
  booktitle = {Proc. USENIX NSDI}, year = {2010}
}

@article{pack11,
  author = {Zohar, Eyal and Cidon, Israel and Mokryn, Osnat},
  title = {The Power of Prediction: Cloud Bandwidth and Cost Reduction},
  journal = {ACM SIGCOMM CCR}, volume = {41}, number = {4}, year = {2011}
}

@inproceedings{smartre09,
  author = {Anand, Ashok and Sekar, Vyas and Akella, Aditya},
  title = {{SmartRE}: An Architecture for Coordinated Network-wide Redundancy Elimination},
  booktitle = {Proc. ACM SIGCOMM}, year = {2009}
}

@inproceedings{sdre12,
  author = {Zhang, Yan and Ansari, Nirwan and Wu, Mingquan and Yu, Heather},
  title = {{SDRE}: Selective Data Redundancy Elimination for Resource Constrained Hosts},
  booktitle = {Proc. IEEE GLOBECOM}, year = {2012}
}

@inproceedings{kml21,
  author = {Akgun, Ibrahim Umit and Aydin, Ali Selman and Shaikh, Aadil and
            Velikov, Lukas and Zadok, Erez},
  title = {A Machine Learning Framework to Improve Storage System Performance},
  booktitle = {Proc. ACM HotStorage}, year = {2021}
}

@article{kml23,
  author = {Akgun, Ibrahim Umit and Aydin, Ali Selman and Burford, Andrew and
            McNeill, Michael and Arkhangelskiy, Michael and Zadok, Erez},
  title = {Improving Storage Systems Using Machine Learning},
  journal = {ACM Transactions on Storage}, volume = {19}, number = {1}, year = {2023}
}

@inproceedings{linnos20,
  author = {Hao, Mingzhe and Toksoz, Levent and Li, Nanqinqin and
            Halim, Edward Edberg and Hoffmann, Henry and Gunawi, Haryadi S.},
  title = {{LinnOS}: Predictability on Unpredictable Flash Storage with a Light Neural Network},
  booktitle = {Proc. USENIX OSDI}, year = {2020}
}

@inproceedings{leap20,
  author = {Al Maruf, Hassan and Chowdhury, Mosharaf},
  title = {Effectively Prefetching Remote Memory with {Leap}},
  booktitle = {Proc. USENIX ATC}, year = {2020}
}

@inproceedings{aiturbo26,
  author = {Hao, Yingyi and others},
  title = {Fast Cloud Storage for {AI} Jobs via Grouped {I/O} API with
           Transparent Read/Write Optimizations},
  booktitle = {Proc. USENIX FAST}, year = {2026}
}

@inproceedings{kaiser16,
  author = {Kaiser, J. and Gad, R. and others},
  title = {Deduplication Potential of {HPC} Applications' Checkpoints},
  booktitle = {Proc. IEEE CLUSTER}, year = {2016}
}
```
