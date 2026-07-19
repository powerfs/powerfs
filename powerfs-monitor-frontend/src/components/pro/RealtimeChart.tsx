/**
 * RealtimeChart — live scrolling line chart.
 *
 * Polls a `fetcher` on a fixed interval, appends samples to an in-memory ring
 * buffer (sliding window), and renders a smooth ECharts line chart that
 * animates as new points arrive. Supports play/pause + clear controls.
 *
 * Theme is derived from ThemeContext (powerfs / powerfs-dark).
 */

import React, { useCallback, useEffect, useRef, useState } from 'react'
import { Card, Space, Button, Tooltip, Tag } from 'antd'
import {
  PlayCircleOutlined,
  PauseCircleOutlined,
  ReloadOutlined,
} from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import { useInterval } from 'ahooks'
import { useTheme } from '@/styles/ThemeContext'
import type { EChartsOption } from 'echarts'
import SkeletonCard from './SkeletonCard'

export interface RealtimeSeries {
  /** Key into the fetcher's returned record */
  key: string
  name: string
  color: string
  /** Show area gradient under the line (default true) */
  area?: boolean
}

export interface RealtimeChartProps {
  /** Polling fetcher returning current values keyed by series.key */
  fetcher: () => Promise<Record<string, number>> | Record<string, number>
  /** Polling interval in ms (default 5000) */
  interval?: number
  /** Max points retained in the sliding window (default 60) */
  maxPoints?: number
  height?: number | string
  series: RealtimeSeries[]
  yAxis?: { min?: number; max?: number; unit?: string }
  title?: React.ReactNode
  /** Show play/pause + reset controls (default true) */
  showControls?: boolean
  loading?: boolean
  style?: React.CSSProperties
}

interface BufferPoint {
  time: number
  values: Record<string, number>
}

function formatTime(ts: number): string {
  const d = new Date(ts)
  const hh = String(d.getHours()).padStart(2, '0')
  const mm = String(d.getMinutes()).padStart(2, '0')
  const ss = String(d.getSeconds()).padStart(2, '0')
  return `${hh}:${mm}:${ss}`
}

/** Convert a #RRGGBB color to an rgba() string with the given alpha. */
function withAlpha(hex: string, alpha: number): string {
  const m = /^#([0-9a-fA-F]{6})$/.exec(hex)
  if (!m) return hex
  const r = parseInt(m[1].slice(0, 2), 16)
  const g = parseInt(m[1].slice(2, 4), 16)
  const b = parseInt(m[1].slice(4, 6), 16)
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

const RealtimeChart: React.FC<RealtimeChartProps> = ({
  fetcher,
  interval = 5000,
  maxPoints = 60,
  height = 220,
  series,
  yAxis,
  title,
  showControls = true,
  loading = false,
  style,
}) => {
  const { resolved } = useTheme()
  const isDark = resolved === 'dark'
  const bufferRef = useRef<BufferPoint[]>([])
  const [, setTick] = useState(0)
  const [paused, setPaused] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const tick = useCallback(async () => {
    try {
      const values = await fetcher()
      const now = Date.now()
      bufferRef.current.push({ time: now, values })
      if (bufferRef.current.length > maxPoints) {
        bufferRef.current = bufferRef.current.slice(-maxPoints)
      }
      setError(null)
      setTick(n => n + 1)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'fetch error')
    }
  }, [fetcher, maxPoints])

  // Initial fetch on mount
  useEffect(() => {
    void tick()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useInterval(() => {
    if (!paused) void tick()
  }, paused ? undefined : interval)

  const reset = () => {
    bufferRef.current = []
    setTick(n => n + 1)
  }

  if (loading) {
    return <SkeletonCard height={height} style={style} />
  }

  const buffer = bufferRef.current
  const times = buffer.map(p => formatTime(p.time))

  const axisLineColor = isDark ? '#334155' : '#E5E7EB'
  const axisLabelColor = isDark ? '#94A3B8' : '#6B7280'
  const splitLineColor = isDark ? '#1E293B' : '#F3F4F6'
  const tooltipBg = isDark ? '#1E293B' : '#FFFFFF'
  const tooltipText = isDark ? '#F1F5F9' : '#1F2937'

  const option: EChartsOption = {
    grid: { left: 48, right: 16, top: series.length > 1 ? 28 : 12, bottom: 28 },
    tooltip: {
      trigger: 'axis',
      backgroundColor: tooltipBg,
      borderColor: axisLineColor,
      borderWidth: 1,
      textStyle: { color: tooltipText, fontSize: 12 },
    },
    legend:
      series.length > 1
        ? {
            data: series.map(s => s.name),
            textStyle: { color: isDark ? '#CBD5E1' : '#4B5563', fontSize: 12 },
            top: 0,
            itemWidth: 12,
            itemHeight: 12,
          }
        : undefined,
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: times,
      axisLine: { lineStyle: { color: axisLineColor } },
      axisLabel: { color: axisLabelColor, fontSize: 11, hideOverlap: true },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      min: yAxis?.min,
      max: yAxis?.max,
      axisLabel: {
        color: axisLabelColor,
        fontSize: 11,
        formatter: yAxis?.unit ? `{value}${yAxis.unit}` : '{value}',
      },
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: { lineStyle: { color: splitLineColor, type: 'dashed' as const } },
    },
    series: series.map(s => ({
      name: s.name,
      type: 'line',
      smooth: true,
      showSymbol: false,
      data: buffer.map(p => p.values[s.key] ?? 0),
      lineStyle: { color: s.color, width: 2 },
      itemStyle: { color: s.color },
      emphasis: { focus: 'series' as const },
      areaStyle:
        s.area === false
          ? undefined
          : {
              color: {
                type: 'linear',
                x: 0,
                y: 0,
                x2: 0,
                y2: 1,
                colorStops: [
                  { offset: 0, color: withAlpha(s.color, 0.28) },
                  { offset: 1, color: withAlpha(s.color, 0.02) },
                ],
              },
            },
    })),
    animation: true,
    animationDuration: 300,
    animationEasing: 'linear',
    animationDurationUpdate: 300,
    animationEasingUpdate: 'linear',
  }

  return (
    <Card
      title={
        <Space size={8}>
          {title}
          {paused && <Tag color="warning">已暂停</Tag>}
          {error && <Tag color="error">数据异常</Tag>}
          {buffer.length > 0 && !paused && !error && (
            <Tag color="processing" style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
              <span
                className="pf-pulse"
                style={{
                  width: 6,
                  height: 6,
                  borderRadius: '50%',
                  background: 'var(--pf-color-success)',
                  display: 'inline-block',
                }}
              />
              实时
            </Tag>
          )}
        </Space>
      }
      extra={
        showControls ? (
          <Space size={4}>
            <Tooltip title={paused ? '继续' : '暂停'}>
              <Button
                type="text"
                size="small"
                icon={paused ? <PlayCircleOutlined /> : <PauseCircleOutlined />}
                onClick={() => setPaused(p => !p)}
              />
            </Tooltip>
            <Tooltip title="清空">
              <Button type="text" size="small" icon={<ReloadOutlined />} onClick={reset} />
            </Tooltip>
          </Space>
        ) : undefined
      }
      style={{ borderRadius: 12, ...style }}
      styles={{ body: { padding: 16 } }}
    >
      <ReactECharts
        option={option}
        notMerge={false}
        lazyUpdate
        style={{ height, width: '100%' }}
        opts={{ renderer: 'canvas' }}
      />
    </Card>
  )
}

export default RealtimeChart