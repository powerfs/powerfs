# 用 Trae 开发 PowerFS 分布式文件系统：问题驱动的工程实践

> 45 分钟技术分享 · 45 页 PPT 提纲

> 核心叙事线：**遇到什么问题 → 怎么解决 → 沉淀成什么**

---

## 第一部分：开场（P1–P3）

### P1 — 封面
- 标题：用 Trae 开发 PowerFS 分布式文件系统
- 副标题：问题驱动下的 AI 辅助系统软件工程实践
- 演讲人 / 日期

### P2 — 议程
- PowerFS 是什么 + 技术栈（3 min）
- 开发环境选型：为什么是 NoMachine + Trae Code（5 min）
- 内核级开发：从死机到 Skill 沉淀（8 min）
- 测试体系：从自由发挥到脚本标准化（8 min）
- 实战案例：从 CI 全红到全绿（12 min）
- 工程实践沉淀（6 min）
- 踩坑与教训（3 min）

### P3 — PowerFS 是什么 + 本次分享视角
- 分布式文件系统：内核态 VFS 模块 + 用户态 Rust 服务 + FUSE 客户端
- 技术栈：C（kernel）+ Rust（10 crate）+ RDMA + YAML/Shell（CI/CD）
- 本次分享**不讲架构细节**，讲开发过程中遇到的真实问题和解法
- 每个问题都遵循：现象 → 根因 → 解法 → 沉淀

---

## 第二部分：开发环境与工具链选型（P4–P8）

### P4 — PowerFS 技术栈全景
- 一张图说清：kernel 模块（.ko）→ Rust 用户态（master/filer/volume/fuse/net）→ CI/CD（YAML + self-hosted runner）
- 跨语言：C / Rust / YAML / Shell / Python（测试脚本）
- 跨平台：内核态 / 用户态 / Docker / VM
- 痛点：上下文跨度大，AI 需要同时理解多个层面

### P5 — 为什么不是 Trae CLI？
- Trae CLI 纯命令行交互，适合脚本化任务
- PowerFS 是系统软件开发：需要看代码、看日志、看 CI 截图、看 diff
- Trae Code（IDE 集成）的优势：
  - 可视化代码浏览：跳转、引用、多文件对比
  - 实时掌握项目进展：文件改动、git status、测试结果一目了然
  - 交互体验：Plan 模式、AskUserQuestion 弹窗、Todo 进度卡片
  - 上下文感知：打开的文件自动进入上下文，不需要手动贴代码
- 核心差异：CLI 是"你问它答"，Trae Code 是"你们一起看代码"

### P6 — NoMachine + Trae Code：远程开发的工作流
- 开发机：Ubuntu host（RDMA 硬件 + Docker + VM）
- 访问方式：NoMachine 远程桌面 → Trae Code IDE
- 为什么用 NoMachine 而不是 SSH + tmux：
  - 内核调试需要看 dmesg / 图形化工具
  - Docker 容器管理用 GUI 更直观
  - NoMachine 低延迟，局域网体验接近本地
- Trae Code 跑在开发机上，所有编译/测试/CI 都在本地 → 零网络延迟

### P7 — 移动端协同：随时掌握进展
- 场景：CI 推送后离开工位（开会/午休），需要随时看结果
- 方式：NoMachine 移动端 / SSH + gh CLI / GitHub Actions App
- Trae Code 的优势：推送后可以在移动端通过 GitHub 看 CI 状态，
  回到工位后 Trae Code 上下文仍在，直接继续修
- 不是"移动端写代码"，而是"移动端监控 + 工位端执行"

### P8 — 工具链全景图
- 一张图：Ubuntu host → NoMachine → Trae Code →（Skill / MCP / Memory / Agent / Shell）
- 双仓库 Git：private（内部开发） + public（开源镜像）
- self-hosted runner：CI 与开发在同一台机器，推送即触发
- 关键：所有工具链都在一台机器上，不存在"本地能跑 CI 跑不了"的割裂

---

## 第三部分：内核级开发——从死机到 Skill 沉淀（P9–P15）

### P9 — 问题：内核模块开发的危险性
- powerfs_mod.ko 直接挂载到 VFS 层，bug = kernel panic = host 死机
- 内核模块涉及：inode/dentry/super_block 生命周期、RDMA 硬件交互、并发 RCU
- 一个空指针解引用 → 整台机器死机 → 丢失所有未保存的代码和上下文

### P10 — 踩坑：早期 Trae 直接在 host 上调试内核
- 初期做法：Trae 直接在 host 上 `insmod powerfs_mod.ko` 测试
- 问题：AI 生成的内核代码有 bug → kernel panic → host 死机
- 死机后果：
  - Trae Code 上下文全部丢失
  - 未提交的代码丢失
  - 需要硬重启，打断开发节奏
- 根因：内核代码直接在 host 运行，没有隔离层

### P11 — 解法：VM 隔离开发环境
- 改为在 QEMU/KVM 虚拟机中编译和测试内核模块
- VM 配置：VFIO 直通 RDMA 网卡 + mTLS 证书
- 内核 panic 只影响 VM，host 不受影响
- Trae Code 在 host 上运行，通过 SSH 向 VM 发命令
- 效果：内核 bug 不再影响开发环境稳定性

### P12 — 沉淀：内核编译 Skill
- 问题：每次编译内核模块的命令序列固定但冗长（make / depmod / insmod / dmesg）
- AI 每次可能"自由发挥"用不同参数 → 不稳定
- 沉淀为 Skill `linux-vfs-kernel-dev`：
  - 固化编译命令：`make -C /lib/modules/$(uname -r)/build M=$(pwd) modules`
  - 固化加载流程：`insmod` → `dmesg` → 检查日志
  - 固化内核开发规范：inode 生命周期、并发控制、内核版本兼容
- 价值：AI 调 Skill 而非自由发挥，内核编译稳定性大幅提升

### P13 — 沉淀：VM 启停 Skill
- 问题：双 VM + RDMA + mTLS 的启动顺序复杂
- 手动启动容易出错（证书不匹配、VFIO 未配置、IB 地址变更）
- 沉淀为 Skill `powerfs-rdma-vm-ops`：
  - VM 启停（QEMU / VFIO 配置）
  - mTLS 证书重签（kernel-client-1 / kernel-client-2）
  - IB 地址变更与 unirdma 双栈共存排障
  - 拓扑切换（单 master/filer 双 VFIO VM ↔ 3+3 HA bridge/TCP）
- 价值：AI 调 Skill 标准化启停，不再每次"发明"流程

### P14 — 内核调试的 AI 协作模式
- 标准协作流程：
  1. AI 修改内核 C 代码（Trae Code Edit）
  2. AI 调 Skill 编译内核模块（Shell + Skill）
  3. AI 通过 SSH 向 VM 加载模块（Shell）
  4. VM 内核 panic → AI 读 dmesg 日志 → 定位 → 修复
  5. 循环直到稳定
- 关键：日志回传给 AI 做分析，但 panic 不影响 host
- 对比早期：从"不敢让 AI 碰内核"到"稳定迭代内核代码"

### P15 — 效果对比
- 早期（host 直接调试）：每次内核 bug = 死机 = 重启 = 丢上下文
  - 一天死机 3-4 次，每次损失 20-30 分钟
  - 不敢让 AI 直接改内核代码
- 现在（VM + Skill）：内核 bug 只影响 VM
  - AI 可以自由迭代内核代码
  - dmesg 日志回传 → AI 自动分析 → 修复
  - 内核开发效率提升 3-5 倍
- 沉淀原则：**高风险操作的隔离层必须固化成 Skill**

---

## 第四部分：测试体系——从自由发挥到脚本标准化（P16–P23）

### P16 — 问题：AI 运行测试的"自由发挥"
- 早期现象：让 AI 跑测试，每次命令不同
  - 有时 `cargo test`，有时 `cargo test --all`
  - 有时加 `--test-threads=1`，有时不加
  - 有时先 `cargo build`，有时直接跑
  - 环境变量、feature flags 随机加
- 后果：测试结果不可比，无法判断"是 bug 还是测试方式变了"
- 根因：AI 没有统一的测试基线，每次都在"即兴创作"

### P17 — 症状：没有基线的连锁问题
- 测试失败不知道是代码问题还是测试命令问题
- AI 为了"让测试通过"可能调整命令而非修复代码
- 回归测试无法自动化（每次命令不同，无法对比）
- 团队协作时测试结果不可复现（每个人/AI 跑的命令不同）

### P18 — 解法：测试脚本标准化
- 策略：把测试流程固化成脚本，AI 改脚本而非直接跑命令
- 分层测试脚本：
  - `scripts/test_unit.sh`：`cargo test -p <crate> --lib`
  - `scripts/test_integration.sh`：`cargo test --all --no-fail-fast`
  - `scripts/test_kernel.sh`：VM 启动 → 模块加载 → 多客户端 FUSE 测试
  - `scripts/test_ci_local.sh`：CI 本地预演（fmt + clippy + build + test）
- AI 的职责：修改脚本内容 / 修改被测代码，而非即兴构造命令
- 效果：测试结果可复现、可对比、可回归

### P19 — 测试分层体系
- L1 单元测试：`cargo test -p powerfs-core --lib`（秒级，开发时频繁跑）
- L2 集成测试：`cargo test --all --no-fail-fast`（分钟级，提交前跑）
- L3 内核多客户端测试：双 VM + mTLS + FUSE 挂载（十分钟级，功能迭代后跑）
- L4 CI 全量：GitHub Actions 8 个 job（推送后自动触发）
- 原则：越靠后的测试越贵，但覆盖面越广
- AI 应该知道"改了什么就跑哪层测试"

### P20 — 沉淀：powerfs-testing Skill
- 问题：测试环境启动复杂（集群启停、FUSE 挂载、VM 配置）
- 沉淀为 Skill `powerfs-testing`：
  - 集群启动（master → filer → volume → fuse）
  - FUSE 客户端挂载与验证
  - 测试执行与结果收集
  - 测试环境清理
- 价值：AI 调 Skill 标准化测试流程，不再每次"发明"测试方法

### P21 — 回归测试：大功能迭代必须完整验证
- 原则：大功能迭代（如锁模型重构、coalescer 异步化）必须跑 L2+L3 全量回归
- 实例：Stage 3 锁重构改了 lock_arbiter 语义 → 12 个测试在干净代码上就失败
  - 原因：重构改了实现语义但测试没同步更新（语义漂移）
  - 如果当时跑了完整回归测试，会立即发现
- 实例：coalescer 异步化改了 record_write 返回值语义 → 旧测试基于同步 API 失败
  - 同样是语义变更未同步测试
- 教训：**语义变更必须同步改测试，不能只改代码**

### P22 — 实例：cap/lock 语义重构后的测试对齐
- 背景：lock_arbiter 的 rdlock 实现了 GATHER 语义（读者等写者 flush）
- 但 12 个测试假设"乐观共存"（读者立即得 CAP_R，不 recall）
- 方法：`git stash` → 干净代码确认是预存失败 → 按实现语义对齐测试
- 提取 `establish_writer_reader_shared` helper：完整走 GATHER 握手
- 验证：`cargo test --all --no-fail-fast` → 93 二进制 / 1839 通过 / 0 失败
- 关键：**有统一测试基线才能判断"是 bug 还是语义变更"**

### P23 — 测试基线的价值总结
- 没有基线：AI 每次自由发挥 → 结果不可比 → 无法判断改动是否引入回归
- 有基线（脚本化 + Skill）：AI 改代码 → 跑标准脚本 → 结果可直接对比
- 基线让"回归测试"成为可能：改了 A，跑全套，如果 B 的测试红了 → A 影响了 B
- 原则：**测试命令不是 AI 的工作，是工程基础设施**

---

## 第五部分：实战案例——从 CI 全红到全绿（P24–P32）

### P24 — 案例背景：CI 8 个 job 全红
- public master 推送后 CI 全红
- CI 配置：`RUSTFLAGS="-D warnings"` 全局严格模式
- 8 个 job：frontend / build / quality / test / coverage / docker / benchmark / deploy
- 问题叠问题：YAML 语法错 → warning → 预存失败测试 → Docker Hub 不可达

### P25 — 问题一：YAML heredoc 导致 0 秒失败
- 现象：workflow job 0 秒结束，无日志
- 根因：heredoc 定界符 `REMOTE_EOF` 顶格破坏 YAML 缩进
- 诊断：workflow 解析阶段就失败，根本没进入执行
- 修复：改用 `ssh ... sh -s "$CERT_NAME" <<'REMOTE_EOF'`（quoted heredoc + 位置参数）
- 教训：**YAML 中嵌入 shell heredoc 必须用 quoted 定界符**

### P26 — 问题二：release-mode 下 18 文件 warning
- 现象：dev 模式不触发，release 模式下 209+ 个 warning
- 分类：
  - FFI 绑定镜像 C ABI（transport_rdma.rs，209 个）→ 文件级 allow
  - 死代码（op_counter / flush_option / 死方法）→ 删除
  - API 迁移（ErrorKind::Other → io::Error::other）→ 修正
  - clippy lint（too_many_arguments / needless_cast / div_ceil）→ 修正
- 验证：`cargo build --release` + `cargo clippy --all-targets -- -D warnings` + `cargo fmt --check`

### P27 — 问题三：清 warning 时发现真实 bug
- 现象：coalescer 禁用模式下 `record_write` 返回 `Some(...)` 强制物化
- 但 `write_needle_blob` 用 `let _ =` 丢弃了返回值
- 后果：写入数据从未落盘 → 读取时 `NeedleNotFound`
- 修复：两处写路径改为 `if let Some(...) { self.flush_coalescer_entry(...)?; }`
- 关键：**清 warning 不只是"消音"，是在读每一行代码，可能发现隐藏 bug**

### P28 — 问题四：12 个预存失败测试（语义漂移）
- 现象：修完 warning 后跑测试，发现 12 个失败
- 溯源：`git stash` → 干净代码上同样失败 → 确认非本次引入
- 两类：
  - 异步 coalescer 测试（基于旧同步 API）→ 改为异步语义
  - cap/lock 测试（乐观共存 vs GATHER）→ 对齐锁语义（见 P22）
- 教训：**`cargo test --all` 遇首个失败即中止，会遮挡后续二进制的失败**
- 解法：调试期用 `--no-fail-fast` 确保每个二进制都执行

### P29 — 问题五：Docker Hub 不可达
- 现象：所有核心 job 绿了，只有 docker job 红
- 根因：self-hosted runner 直连 `registry-1.docker.io` 超时
- 探测：本机配了 ustc/163 mirror 但已失效
- 测试 7 个候选镜像源 → daocloud 和 1ms.run 可用
- 修复：workflow 改为本地缓存→直连→daocloud→1ms.run 回退链
- 教训：**CI 的最后一公里（镜像/部署）容易被忽视**

### P30 — 解法：分层修复 + 逐步验证 + 推送
- 3 个 commit 按逻辑分层：
  1. `fix(ci): clear release-mode warnings and materialise disabled-coalescer writes`
  2. `test(filer): align cap/lock integration tests with rdlock GATHER semantics`
  3. `ci(docker): pull ubuntu base through reachable registry mirrors`
- 每层修复后立即验证：fmt → clippy → 对应测试 → 推送 → 监控 CI
- 双远端推送：`git push private master` + `git push https://github.com/powerfs/powerfs.git master:master`
- 用 `gh run view` 监控 CI，失败 job 用 `gh run rerun --failed` 重跑

### P31 — 结果：CI 全绿
- 8 个 job 全绿：frontend ✓ build ✓ quality ✓ test ✓（29m）coverage ✓ docker ✓ benchmark ✓ deploy ✓
- 全套测试：93 二进制 / 1839 通过 / 0 失败 / 15 ignored
- 双远端 HEAD 一致：`24fd5a37`
- 从诊断到全绿约 3 小时

### P32 — 复盘：Trae 的加速点
- 并行验证：build + clippy + fmt 同时跑
- 子代理搜索：Explore agent 做广度搜索，主上下文只留结论
- Memory 保持一致性：跨 3 次 context 压缩，memory 保证不丢约定
- Skill 标准化：测试/CI 验证走 Skill，不自由发挥
- 关键：**AI 的价值不只是写代码快，是"搜索→分析→修复→验证"全链路快**

---

## 第六部分：工程实践沉淀（P33–P38）

### P33 — Skill 沉淀：把重复操作变成可执行工作流
- 原则：**任何 AI 做了两次以上的操作，第三次就应该沉淀成 Skill**
- PowerFS 的 Skill 矩阵：
  - `powerfs-ci-local`：推送前本地 CI 预演（6 项检查）
  - `powerfs-testing`：测试环境启停 + 执行
  - `powerfs-rdma-vm-ops`：VM 启停 + RDMA + mTLS
  - `linux-vfs-kernel-dev`：内核编译 + 加载 + 调试规范
  - `code-quality`：Rust 全量质量检查
- 价值：AI 调 Skill = 走标准流程，不调 Skill = 自由发挥（不稳定）
- 沉淀路径：手动操作 → 脚本 → Skill → AI 自动调用

### P34 — Memory：跨会话保持项目约定
- 问题：Trae 上下文窗口有限，长任务会触发 context 压缩
- 压缩后 AI 不记得：改了哪些文件、验证到哪一步、远端配置是什么
- 解法：Memory 系统三层：
  - `project_memory.md`：项目约定（远端配置、测试命令、VM 端口）
  - `user_profile.md`：用户偏好（中文、conventional commits、彻底修复）
  - `session_memory` + `topics.md`：会话级记忆
- 实例：CI 修复跨 3 次 context 压缩，memory 保证约定不丢
- 原则：**重要约定写进 memory，不依赖对话记忆**

### P35 — 提交粒度与 commit message
- 一个逻辑变更一个 commit（warning 修复 / 测试对齐 / docker 修复分开）
- conventional commits：`fix(ci):` / `test(filer):` / `ci(docker):`
- message body 写"为什么"而非"做了什么"（代码本身已说明 what）
- 示例：disabled-coalescer bug 的 commit message 解释了根因和影响
- 原则：**AI 可以起草 commit message，但人必须审核后再提交**

### P36 — 上下文管理策略
- 大型项目（17 crate + kernel 子模块）上下文管理：
  - Explore 子代理做广度搜索，主上下文只留结论
  - new_context 在里程碑后切换，带精炼 handoff note
  - Grep/Glob 做精确定位，避免全文件 Read
  - TodoWrite 追踪进度，context 压缩后仍可恢复
- 原则：**上下文是稀缺资源，搜索和分析交给子代理，主上下文只做决策**

### P37 — CI 失败的分层处理
- 第一层：编译/格式/clippy → 本地 `cargo` 全量验证后推送
- 第二层：测试失败 → `--no-fail-fast` 确保不遗漏被遮挡的二进制
- 第三层：基础设施失败（Docker Hub 超时）→ 与代码无关，单独修 workflow
- 用 `gh run view` 监控，`gh run rerun --failed` 重跑瞬时失败
- 原则：**区分"代码问题"和"基础设施问题"，不要混在一起修**

### P38 — 什么时候不该用 AI
- 纯执行型任务（跑一条命令）→ 直接 Shell，不需要 AI
- 已知精确修改（改一行 typo）→ 直接 Edit，不需要 Plan 模式
- 安全敏感操作（force push、删分支）→ AI 只建议，人执行
- 内核硬件交互（RDMA 配置、BIOS 设置）→ AI 不懂硬件，人工操作
- 原则：**AI 的价值在于"减少理解成本"，简单任务直接做更快**

---

## 第七部分：踩坑与教训（P39–P43）

### P39 — 坑一：host 上调试内核导致死机
- 问题：早期 Trae 直接在 host 上 `insmod` 内核模块
- 后果：内核 bug → kernel panic → host 死机 → 丢失上下文和未提交代码
- 解法：VM 隔离 + 内核编译/加载 Skill 沉淀
- 通用教训：**高风险操作必须有隔离层，不能在生产环境直接试**

### P40 — 坑二：AI 自由发挥测试命令
- 问题：每次让 AI 跑测试，命令参数不同
- 后果：结果不可比，无法判断"是 bug 还是测试方式变了"
- 解法：测试脚本标准化 + Skill 沉淀，AI 改脚本而非直接跑命令
- 通用教训：**重复的操作必须标准化，不能让 AI 每次即兴创作**

### P41 — 坑三：cargo test --all 的失败遮挡
- 问题：`cargo test --all` 遇首个失败 binary 即中止
- 后果：修好一批测试后发现"新"的失败（其实是被遮挡的预存失败）
- 解法：调试期用 `--no-fail-fast`，确认全量后再推送
- 通用教训：**测试工具的默认行为可能隐藏问题，必须了解工具语义**

### P42 — 坑四：测试与实现语义漂移
- 问题：Stage 3 锁重构改了 lock_arbiter 语义，但测试没同步更新
- 后果：12 个测试在干净代码上就失败，但没人知道（因为没跑完整回归）
- 解法：大功能迭代后跑完整回归测试，用 `git stash` 区分新失败和预存失败
- 通用教训：**重构后必须跑完整测试套件，语义变更必须同步改测试**

### P43 — 坑五：context 压缩丢失中间状态
- 问题：长任务（CI 修复跨 3 小时）触发自动 context 压缩
- 后果：压缩后不记得改了哪些文件、验证到哪一步
- 解法：memory 记录关键约定 + TodoWrite 追踪进度 + new_context 带 handoff
- 通用教训：**长任务必须有持久化状态追踪，不能依赖对话记忆**

---

## 第八部分：展望与 Q&A（P44–P45）

### P44 — 展望：AI 辅助系统软件开发的方向
- 当前局限：AI 对内核调试（ftrace/kgdb）支持有限，RDMA 硬件交互需人工
- Skill 生态的进化方向：从"固化命令"到"固化决策逻辑"
- 测试自动化：AI 改代码 → 自动判断跑哪层测试 → 自动执行 → 自动分析结果
- PowerFS 后续：RDMA 多路径、WAL 故障恢复、HA 自动切换
- 愿景：AI 不只写代码，而是全生命周期协作（设计→开发→测试→部署→运维）

### P45 — Q&A
- 感谢
- 联系方式
- 仓库链接：github.com/powerfs/powerfs
- 提问环节

---

## 附录：演讲节奏建议

| 部分 | 页数 | 时长 | 节奏 | 核心 |
|------|------|------|------|------|
| 开场 | P1–P3 | 3 min | 快速过 | 建立问题驱动共识 |
| 开发环境 | P4–P8 | 5 min | 截图为主 | NoMachine + Trae Code + 移动端 |
| 内核开发 | P9–P15 | 8 min | 问题→解法→沉淀 | 死机→VM→Skill |
| 测试体系 | P16–P23 | 8 min | 问题→解法→沉淀 | 自由发挥→脚本→基线 |
| 实战案例 | P24–P32 | 12 min | 重点详讲 | 5 个问题分层修复 |
| 工程实践 | P33–P38 | 6 min | 干货密集 | Skill / Memory / 提交 / 上下文 |
| 踩坑 | P39–P43 | 3 min | 快速引发共鸣 | 5 个坑 |
| 展望 Q&A | P44–P45 | 1 min | 收尾 | |

## 附录：关键 commit 引用

| Commit | 说明 |
|--------|------|
| `9fe1d84b` | fix(ci): repair kernel-multi-client workflow YAML after cert change |
| `2ba463de` | fix(ci): clear release-mode warnings and materialise disabled-coalescer writes |
| `f168eb92` | test(filer): align cap/lock integration tests with rdlock GATHER semantics |
| `24fd5a37` | ci(docker): pull ubuntu base through reachable registry mirrors |

## 附录：核心叙事线

```
问题 → 根因 → 解法 → 沉淀

内核开发：
  死机 → host 直接 insmod → VM 隔离 → 内核编译/VM 启停 Skill

测试体系：
  结果不可比 → AI 自由发挥命令 → 脚本标准化 → 测试 Skill + 基线

CI 修复：
  8 job 全红 → 5 层问题叠加 → 分层修复 → Skill + Memory + 标准流程

工程实践：
  重复操作不稳定 → AI 即兴创作 → Skill 沉淀 → 标准化工作流
```
