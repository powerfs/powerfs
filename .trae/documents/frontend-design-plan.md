# PowerFS Monitor 前端专业美观性升级规划

> 版本: v1.0 | 日期: 2026-07-20
> 范围: `powerfs-monitor-frontend`
> 目标: 从 antd 默认风格升级为 Grafana/Datadog 级专业监控产品

---

## 一、现状盘点

### 1.1 技术栈
- React 18 + TypeScript 5 + Vite 8
- Ant Design 5.20（UI 库）
- ECharts 5.6（图表）
- React Router 6 + axios + dayjs + Sass

### 1.2 现有页面（15 个）
Dashboard、Nodes、StorageDevices、Volumes、BitrotScrub、Fuse、Conflicts、KV、S3、Alerts、AccessKeys、Users、Roles、Login、Optimizations

### 1.3 视觉短板
| 维度 | 现状 | 问题 |
|---|---|---|
| 主题 | antd 默认浅色 | 无品牌识别度 |
| 字体 | 系统默认 | 数字不等宽，缺科技感 |
| 卡片 | 朴素 Card | 无 sparkline / 状态条 / delta |
| 图表 | ECharts 默认 | 无定制主题，无实时滚动 |
| 拓扑 | 无 | raft/rack 拓扑无法可视化 |
| 空状态 | "暂无数据" | 无插画无引导 |
| 加载 | Spinner | 无骨架屏 |
| 动效 | 无 | 数字无滚动，状态无过渡 |
| 大屏 | 无 | 不能投屏 NOC |
| 快捷键 | 无 | 无 Cmd+K 全局搜索 |
| 侧栏 | 平铺 15 项 | 无分组，信息过载 |

---

## 二、设计目标

对标业界顶级监控产品：
- **Grafana**: 暗色 + 多面板 + 实时刷新 + 可拖拽
- **Datadog**: 卡片化 + 时间线 + 颜色编码
- **Linear**: 玻璃拟态 + 微交互 + 键盘优先
- **Vercel Dashboard**: 极简 + 高对比度 + 动效克制

**双主题**：明色（白天办公）+ 暗色（NOC 监控墙）+ 跟随系统。

---

## 三、技术选型

```
UI 库:        Ant Design 5 + @ant-design/pro-components
图表:         ECharts 5（定制主题）+ @ant-design/charts
拓扑:         react-flow（raft/rack 拓扑）
动效:         framer-motion + react-countup
样式:         Sass + CSS 变量（双主题）
字体:         Inter（正文）+ JetBrains Mono（数字）
状态:         ahooks useRequest
构建:         Vite 8
```

新增依赖：
```json
{
  "@ant-design/pro-components": "^2.7",
  "@ant-design/charts": "^2.2",
  "reactflow": "^11",
  "framer-motion": "^11",
  "react-countup": "^6",
  "ahooks": "^3"
}
```

---

## 四、设计系统

### 4.1 设计令牌（Design Tokens）

文件：`src/styles/tokens.ts`

```typescript
export const tokens = {
  color: {
    brand: '#1677FF',
    brandGradient: 'linear-gradient(135deg, #1677FF 0%, #722ED1 100%)',
    success: '#52C41A',
    warning: '#FAAD14',
    danger: '#FF4D4F',
    info: '#1890FF',
    neutral: { 50: '#F9FAFB', 100: '#F3F4F6', /* ... */ 900: '#111827' },
  },
  radius: { sm: 4, md: 8, lg: 12, xl: 16 },
  shadow: {
    card: '0 1px 3px rgba(0,0,0,0.06)',
    hover: '0 4px 12px rgba(0,0,0,0.08)',
    pop: '0 8px 24px rgba(0,0,0,0.12)',
  },
  spacing: { xs: 4, sm: 8, md: 16, lg: 24, xl: 32, xxl: 48 },
  fontSize: { xs: 12, sm: 14, md: 16, lg: 18, xl: 20, xxl: 24, display: 32 },
}
```

### 4.2 状态色系统

文件：`src/styles/status.ts`

```typescript
export const statusColor = {
  pending:    { bg, border, text, dot },
  active:     { bg, border, text, dot },
  cordoned:   { bg, border, text, dot },
  draining:   { bg, border, text, dot },
  removed:    { bg, border, text, dot },
  unreachable:{ bg, border, text, dot },
}
```

每个状态 4 个层级（背景/边框/文字/圆点），保证 Tag、徽标、拓扑节点配色一致。

### 4.3 双主题切换

- `ThemeProvider` 上下文管理主题状态
- antd `ConfigProvider` + `theme.darkAlgorithm` / `theme.defaultAlgorithm`
- CSS 变量切换：`document.documentElement.dataset.theme = 'dark' | 'light'`
- 持久化到 localStorage，默认跟随 `prefers-color-scheme`
- 顶栏切换按钮（明/暗/跟随系统三态）

### 4.4 字体

```css
@font-face {
  font-family: 'Inter';
  src: url('/fonts/Inter-Variable.woff2') format('woff2');
}
@font-face {
  font-family: 'JetBrains Mono';
  src: url('/fonts/JetBrainsMono-Variable.woff2') format('woff2');
}

:root {
  --font-sans: 'Inter', -apple-system, 'PingFang SC', sans-serif;
  --font-mono: 'JetBrains Mono', 'SF Mono', Consolas, monospace;
}

.font-num {
  font-family: var(--font-mono);
  font-variant-numeric: tabular-nums;
}
```

---

## 五、组件库扩展

### 5.1 自建 Pro 组件

文件路径：`src/components/pro/`

| 组件 | 用途 |
|---|---|
| `StatCard` | 统计卡片（带 sparkline + delta + 状态条） |
| `StatusTag` | 状态标签（统一配色 + 圆点） |
| `MetricChart` | 指标图表（封装 ECharts + 自动主题） |
| `TopologyGraph` | 拓扑图（封装 react-flow） |
| `TimeSeriesTable` | 时序表格（自动刷新 + 历史对比） |
| `EmptyState` | 空状态（带插画 + 引导操作） |
| `SkeletonCard` | 骨架屏卡片 |
| `DrawerDetail` | 抽屉详情（替代部分 Modal） |
| `KpiBar` | 顶部 KPI 横条 |
| `RefreshControl` | 刷新控件（手动 + 自动 + 倒计时） |

### 5.2 StatCard 设计

```tsx
<StatCard
  title="活跃节点"
  value={128}
  delta={+3}
  deltaType="up"
  sparkline={[...]}
  icon={<CloudServerOutlined />}
  status="healthy"
  onClick={() => navigate('/nodes')}
/>
```

视觉特点：
- 顶部 4px 渐变色条（按状态变色）
- 大数字（32px）+ 等宽字体
- 同比/环比箭头 + 颜色
- 右下角 sparkline 迷你图
- hover 时阴影上浮 + 边框高亮

---

## 六、信息架构升级

### 6.1 侧栏分组

```
📊 总览
  ├ 仪表盘
  ├ 告警中心

🏗️ 基础设施
  ├ 节点管理
  ├ 存储设备
  ├ Master 集群        ← P2
  └ FUSE 客户端        ← P2

💾 存储
  ├ Volume 管理
  ├ Bitrot 扫描
  ├ S3 管理

🔄 元数据
  ├ 冲突管理
  ├ 拓扑视图          ← P2
  └ 卷分配可视化      ← P2

🔐 安全
  ├ 用户管理
  ├ 角色管理
  ├ 我的密钥

⚡ 性能
  ├ 优化开关
  └ KV 管理
```

### 6.2 顶栏全局组件

- **全局搜索**（Cmd+K）：快速跳转节点/卷/告警
- **集群健康度徽章**：右上角绿/黄/红
- **最后刷新时间** + 手动刷新 + 自动刷新开关
- **主题切换**（明/暗/跟随系统）
- **用户菜单**

---

## 七、数据可视化升级

### 7.1 ECharts 定制主题

文件：`src/styles/echarts.theme.ts`

```typescript
export const powerfsEchartsTheme = {
  color: ['#1677FF', '#52C41A', '#FAAD14', '#FF4D4F', '#722ED1', '#13C2C2'],
  backgroundColor: 'transparent',
  textStyle: { fontFamily: 'Inter, PingFang SC' },
  categoryAxis: { axisLine: { lineStyle: { color: '#595959' } } },
  animationDuration: 300,
}
```

注册：`echarts.registerTheme('powerfs', powerfsEchartsTheme)`

### 7.2 图表类型对应

| 场景 | 图表 | 库 |
|---|---|---|
| raft 节点关系 / rack 拓扑 | 拓扑图 | react-flow |
| 卷分配流向 | 桑基图 | ECharts |
| 节点负载矩阵 | 热力图 | ECharts |
| 集群健康度 | 仪表盘 | ECharts gauge |
| 冲突解决历史 | 时间线 | antd Timeline |
| 实时指标 | 平滑滚动折线 | ECharts + WebSocket |

### 7.3 实时数据流

- WebSocket 推流，图表平滑追加而非全量重绘
- 数字变化用 `react-countup` 滚动动效
- 状态变化用 `framer-motion` 过渡
- 图表加载用骨架屏

---

## 八、实施计划

### 8.1 P0（地基，1 周）

| # | 任务 | 输出 |
|---|---|---|
| 1 | 安装依赖 | package.json 更新 |
| 2 | 设计令牌 | `src/styles/tokens.ts` |
| 3 | 状态色系统 | `src/styles/status.ts` |
| 4 | 双主题切换 | `ThemeProvider` + `main.tsx` 改造 |
| 5 | 字体引入 | `public/fonts/` + CSS |
| 6 | 全局样式重写 | `src/index.css` + CSS 变量 |
| 7 | Pro 组件库 | `src/components/pro/*` |
| 8 | Dashboard KPI 升级 | `pages/Dashboard/index.tsx` |

### 8.2 P1（信息架构 + 体验，2 周）

| # | 任务 | 输出 |
|---|---|---|
| 9 | Layout 侧栏分组 | `components/Layout/index.tsx` |
| 10 | 顶栏全局组件 | 健康度徽章 + 刷新控件 + 主题切换 |
| 11 | Cmd+K 全局搜索 | `components/GlobalSearch/` |
| 12 | ECharts 主题定制 | `styles/echarts.theme.ts` |
| 13 | 表格增强 | 密度切换 + 骨架屏 + 空状态 |
| 14 | Nodes 页面升级 | 状态机字段 + 熔断器 |
| 15 | 抽屉详情 | `components/pro/DrawerDetail` |

### 8.3 P2（可视化亮点，3 周）

| # | 任务 | 输出 |
|---|---|---|
| 16 | react-flow 拓扑图 | `components/pro/TopologyGraph` |
| 17 | 节点拓扑视图页 | `pages/Topology/` |
| 18 | Master 集群状态页 | `pages/MasterCluster/` |
| 19 | FUSE 客户端连接页 | `pages/FuseClients/` |
| 20 | 桑基图卷分配 | `pages/AssignSankey/` |
| 21 | 大屏模式 | `components/BigScreen/` |

### 8.4 P3（打磨，1 周）

| # | 任务 |
|---|---|
| 22 | 数字滚动 + 状态动效 |
| 23 | 错误边界 + 网络断开提示 |
| 24 | 国际化（中英双语） |
| 25 | 无障碍（ARIA + 键盘导航） |

---

## 九、验证方法

### 9.1 代码质量检查

```bash
cd powerfs-monitor-frontend
pnpm install              # 安装依赖
pnpm run build            # tsc + vite build，必须通过
pnpm run dev              # 本地启动，手动验证
```

### 9.2 视觉验证

- 明暗主题切换无闪烁
- 所有页面在 1366/1920/2560 分辨率下正常
- 状态色在所有页面一致
- 字体在数字位置等宽对齐
- 骨架屏与最终内容布局一致

### 9.3 交互验证

- Cmd+K 全局搜索可用
- 主题切换持久化
- 表格密度切换持久化
- 实时数据流平滑无闪烁
- 错误状态有友好提示

---

## 十、风险与权衡

| 风险 | 缓解 |
|---|---|
| Pro Components 增加 bundle | tree-shaking + 按需引入 |
| react-flow 学习成本 | 拓扑图是监控核心，值得投入 |
| 暗色模式改造工作量大 | 新页面直接用 tokens，老页面渐进迁移 |
| 字体加载增加首屏 | 用 `font-display: swap` + woff2 |
| ECharts 主题需统一注册 | 在 main.tsx 注册一次全局生效 |

---

## 十一、回滚策略

- 所有改动在 `frontend-redesign` 分支进行
- 每个阶段独立 commit，可单独回滚
- 保留 `package.json` 原始版本备份
- 出现构建问题立即