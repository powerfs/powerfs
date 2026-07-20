/**
 * StatCard — KPI statistic card with sparkline, delta indicator and status bar.
 *
 * Usage:
 *   <StatCard title="活跃节点" value={128} delta={+3} status="active"
 *     sparkline={[12,13,15,...]} icon={<CloudServerOutlined />}
 *     onClick={() => navigate('/nodes')} />
 */

import React from 'react'
import { Card, Typography } from 'antd'
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import type { NodeStatus } from '@/styles/status'
import { resolveNodeStatus } from '@/styles/status'
import { useTheme } from '@/styles/ThemeContext'

const { Text } = Typography

export interface StatCardProps {
  title: string
  value: number
  /** Decimal places for the displayed number */
  precision?: number
  /** Optional suffix (e.g. "%", "GB") */
  suffix?: string
  /** Optional prefix */
  prefix?: React.ReactNode
  /** Delta vs. previous period; positive = up, negative = down */
  delta?: number
  /** Treat positive delta as bad (e.g. error rate). Defaults to false. */
  invertDelta?: boolean
  /** Status drives the top color bar */
  status?: NodeStatus | string
  /** Optional sparkline data */
  sparkline?: number[]
  /** Icon element shown top-right */
  icon?: React.ReactNode
  /** Click handler — turns the card into a navigation entry */
  onClick?: () => void
  /** Loading state */
  loading?: boolean
  /** Extra footer content */
  footer?: React.ReactNode
}

const StatCard: React.FC<StatCardProps> = ({
  title,
  value,
  precision = 0,
  suffix,
  prefix,
  delta,
  invertDelta = false,
  status = 'active',
  sparkline,
  icon,
  onClick,
  loading = false,
  footer,
}) => {
  const { resolved } = useTheme()
  const palette = resolveNodeStatus(status)

  const deltaIsUp = (delta ?? 0) > 0
  const deltaIsGood = invertDelta ? !deltaIsUp : deltaIsUp
  const deltaColor = deltaIsGood
    ? 'var(--pf-color-success)'
    : 'var(--pf-color-danger)'

  const sparkOption: EChartsOption | null = sparkline?.length
    ? {
        animation: false,
        grid: { top: 4, left: 0, right: 0, bottom: 0 },
        xAxis: { type: 'category', show: false, boundaryGap: false },
        yAxis: { type: 'value', show: false, scale: true },
        tooltip: { show: false },
        series: [
          {
            type: 'line',
            data: sparkline,
            smooth: true,
            symbol: 'none',
            lineStyle: { color: palette.dot, width: 2 },
            areaStyle: {
              color: {
                type: 'linear',
                x: 0,
                y: 0,
                x2: 0,
                y2: 1,
                colorStops: [
                  { offset: 0, color: `${palette.dot}33` },
                  { offset: 1, color: `${palette.dot}00` },
                ],
              },
            },
          },
        ],
      }
    : null

  return (
    <Card
      hoverable={!!onClick}
      onClick={onClick}
      loading={loading}
      style={{
        position: 'relative',
        overflow: 'hidden',
        cursor: onClick ? 'pointer' : 'default',
        borderRadius: 12,
      }}
      styles={{ body: { padding: 20 } }}
    >
      {/* Top status color bar */}
      <div
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          height: 3,
          background: palette.dot,
          opacity: 0.85,
        }}
      />

      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: 8,
        }}
      >
        <Text type="secondary" style={{ fontSize: 13 }}>
          {title}
        </Text>
        {icon && (
          <span
            style={{
              width: 36,
              height: 36,
              borderRadius: 8,
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              background: 'var(--pf-gradient-brand-soft)',
              color: 'var(--pf-color-brand)',
              fontSize: 18,
            }}
          >
            {icon}
          </span>
        )}
      </div>

      <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
        {prefix && <span style={{ fontSize: 16 }}>{prefix}</span>}
        <span
          className="tabular-nums"
          style={{
            fontSize: 32,
            fontWeight: 600,
            lineHeight: 1.1,
            color: 'var(--pf-color-text)',
          }}
        >
          {value.toLocaleString('zh-CN', { minimumFractionDigits: precision, maximumFractionDigits: precision })}
        </span>
        {suffix && (
          <Text type="secondary" style={{ fontSize: 14 }}>
            {suffix}
          </Text>
        )}
      </div>

      {/* Delta + sparkline row */}
      <div
        style={{
          marginTop: 8,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: 12,
        }}
      >
        {delta !== undefined && delta !== 0 ? (
          <span
            style={{
              color: deltaColor,
              fontSize: 12,
              display: 'inline-flex',
              alignItems: 'center',
              gap: 4,
            }}
          >
            {deltaIsUp ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
            {Math.abs(delta).toFixed(1)}%
          </span>
        ) : (
          <span style={{ fontSize: 12, color: 'var(--pf-color-text-tertiary)' }}>
            —
          </span>
        )}

        {sparkOption && (
          <div style={{ width: 80, height: 32 }}>
            <ReactECharts
              option={sparkOption}
              theme={resolved === 'dark' ? 'powerfs-dark' : 'powerfs'}
              style={{ width: '100%', height: '100%' }}
              opts={{ renderer: 'svg' }}
            />
          </div>
        )}
      </div>

      {footer && (
        <div
          style={{
            marginTop: 12,
            paddingTop: 12,
            borderTop: '1px solid var(--pf-color-border-secondary)',
          }}
        >
          {footer}
        </div>
      )}
    </Card>
  )
}

export default StatCard