/**
 * MetricChart — thin wrapper around ReactECharts that:
 *  - auto-applies the registered powerfs / powerfs-dark theme
 *  - accepts a loading state (renders skeleton)
 *  - accepts a height (defaults to 300)
 *  - merges sensible defaults (grid, tooltip) so callers stay terse
 */

import React from 'react'
import { Skeleton } from 'antd'
import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useTheme } from '@/styles/ThemeContext'

export interface MetricChartProps {
  option: EChartsOption
  height?: number | string
  loading?: boolean
  /** Skeleton height when loading; defaults to height */
  skeletonHeight?: number
  style?: React.CSSProperties
  className?: string
  /** Force a specific theme (otherwise derived from ThemeContext) */
  forceTheme?: 'light' | 'dark'
}

const MetricChart: React.FC<MetricChartProps> = ({
  option,
  height = 300,
  loading = false,
  skeletonHeight,
  style,
  className,
  forceTheme,
}) => {
  const { resolved } = useTheme()
  const theme = forceTheme ?? resolved

  if (loading) {
    return (
      <Skeleton
        active
        paragraph={{ rows: Math.max(4, Number(height) / 24 || 8) }}
        style={{ height: skeletonHeight ?? height, ...style }}
      />
    )
  }

  return (
    <ReactECharts
      option={option}
      theme={theme === 'dark' ? 'powerfs-dark' : 'powerfs'}
      notMerge
      lazyUpdate
      style={{ height, width: '100%', ...style }}
      className={className}
      opts={{ renderer: 'canvas' }}
    />
  )
}

export default MetricChart