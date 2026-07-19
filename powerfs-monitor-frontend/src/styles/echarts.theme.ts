/**
 * ECharts theme for PowerFS Monitor.
 *
 * Register once at app startup via `registerEChartsTheme()`.
 * Use `<ReactECharts theme="powerfs" />` (light) or
 * `<ReactECharts theme="powerfs-dark" />` (dark).
 */

import type { EChartsOption } from 'echarts'
import { tokens } from './tokens'

/** ECharts theme objects are untyped dictionaries in the public API. */
type ThemeOption = Record<string, unknown>

const palette = [
  tokens.color.brand,
  tokens.color.success,
  tokens.color.warning,
  tokens.color.danger,
  tokens.color.purple,
  tokens.color.cyan,
  '#FA8C16',
  '#EB2F96',
]

const baseGrid = {
  top: 32,
  left: 12,
  right: 16,
  bottom: 8,
  containLabel: true,
}

const baseTooltip: EChartsOption['tooltip'] = {
  trigger: 'axis',
  backgroundColor: 'rgba(15, 23, 42, 0.92)',
  borderColor: 'rgba(22, 119, 255, 0.4)',
  borderWidth: 1,
  padding: [8, 12],
  textStyle: {
    color: '#F1F5F9',
    fontSize: tokens.fontSize.xs,
    fontFamily: tokens.fontFamily.sans,
  },
  axisPointer: {
    type: 'line',
    lineStyle: {
      type: 'dashed',
      color: tokens.color.brand,
      width: 1,
    },
  },
}

export const powerfsLightTheme: ThemeOption = {
  color: palette,
  backgroundColor: 'transparent',
  textStyle: {
    fontFamily: tokens.fontFamily.sans,
    color: tokens.color.neutral[700],
  },
  title: {
    textStyle: {
      color: tokens.color.neutral[900],
      fontSize: tokens.fontSize.lg,
      fontWeight: 600,
    },
    subtextStyle: {
      color: tokens.color.neutral[500],
      fontSize: tokens.fontSize.xs,
    },
  },
  legend: {
    textStyle: {
      color: tokens.color.neutral[600],
      fontSize: tokens.fontSize.xs,
    },
    icon: 'roundRect',
    itemWidth: 12,
    itemHeight: 8,
    itemGap: 16,
  },
  grid: baseGrid,
  tooltip: baseTooltip,
  categoryAxis: {
    axisLine: { lineStyle: { color: tokens.color.neutral[300] } },
    axisTick: { show: false },
    axisLabel: {
      color: tokens.color.neutral[500],
      fontSize: tokens.fontSize.xs,
      fontFamily: tokens.fontFamily.mono,
    },
    splitLine: { show: false },
  },
  valueAxis: {
    axisLine: { show: false },
    axisTick: { show: false },
    axisLabel: {
      color: tokens.color.neutral[500],
      fontSize: tokens.fontSize.xs,
      fontFamily: tokens.fontFamily.mono,
    },
    splitLine: {
      lineStyle: {
        type: 'dashed',
        color: tokens.color.neutral[200],
      },
    },
  },
  line: {
    smooth: true,
    symbol: 'circle',
    symbolSize: 6,
    lineStyle: { width: 2 },
    emphasis: { focus: 'series' },
  },
  bar: {
    itemStyle: { borderRadius: [4, 4, 0, 0] },
  },
  pie: {
    itemStyle: {
      borderColor: '#FFFFFF',
      borderWidth: 2,
    },
    label: {
      color: tokens.color.neutral[700],
      fontSize: tokens.fontSize.xs,
    },
  },
}

export const powerfsDarkTheme: ThemeOption = {
  ...powerfsLightTheme,
  backgroundColor: 'transparent',
  textStyle: {
    fontFamily: tokens.fontFamily.sans,
    color: '#CBD5E1',
  },
  title: {
    textStyle: {
      color: '#F1F5F9',
      fontSize: tokens.fontSize.lg,
      fontWeight: 600,
    },
    subtextStyle: {
      color: '#94A3B8',
      fontSize: tokens.fontSize.xs,
    },
  },
  legend: {
    textStyle: {
      color: '#CBD5E1',
      fontSize: tokens.fontSize.xs,
    },
    icon: 'roundRect',
    itemWidth: 12,
    itemHeight: 8,
    itemGap: 16,
  },
  categoryAxis: {
    axisLine: { lineStyle: { color: '#334155' } },
    axisTick: { show: false },
    axisLabel: {
      color: '#94A3B8',
      fontSize: tokens.fontSize.xs,
      fontFamily: tokens.fontFamily.mono,
    },
    splitLine: { show: false },
  },
  valueAxis: {
    axisLine: { show: false },
    axisTick: { show: false },
    axisLabel: {
      color: '#94A3B8',
      fontSize: tokens.fontSize.xs,
      fontFamily: tokens.fontFamily.mono,
    },
    splitLine: {
      lineStyle: {
        type: 'dashed',
        color: '#1E293B',
      },
    },
  },
  pie: {
    itemStyle: {
      borderColor: '#0F172A',
      borderWidth: 2,
    },
    label: {
      color: '#CBD5E1',
      fontSize: tokens.fontSize.xs,
    },
  },
}

let registered = false

/**
 * Register both light & dark themes with ECharts.
 * Safe to call multiple times; only registers once.
 */
export function registerEChartsTheme(
  echarts: typeof import('echarts'),
): void {
  if (registered) return
  echarts.registerTheme('powerfs', powerfsLightTheme as never)
  echarts.registerTheme('powerfs-dark', powerfsDarkTheme as never)
  registered = true
}