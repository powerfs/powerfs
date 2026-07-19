/**
 * RefreshControl — manual + auto refresh widget.
 *
 * Shows the last refresh timestamp, a manual refresh button, and a dropdown
 * to select auto-refresh interval (off / 5s / 10s / 30s / 1m).
 * Uses ahooks `useInterval` for the timer.
 */

import React, { useState, useEffect, useCallback } from 'react'
import { Button, Dropdown, Space, Tooltip, Tag } from 'antd'
import type { MenuProps } from 'antd'
import {
  ReloadOutlined,
  ClockCircleOutlined,
  DownOutlined,
} from '@ant-design/icons'
import { useInterval } from 'ahooks'
import dayjs from 'dayjs'
import { useTheme } from '@/styles/ThemeContext'

export interface RefreshControlProps {
  onRefresh: () => void | Promise<void>
  /** Default auto-refresh interval in ms; 0 means off */
  defaultInterval?: number
  /** Loading state (disables the manual button) */
  loading?: boolean
}

const INTERVAL_OPTIONS: { label: string; value: number }[] = [
  { label: '关闭', value: 0 },
  { label: '5 秒', value: 5_000 },
  { label: '10 秒', value: 10_000 },
  { label: '30 秒', value: 30_000 },
  { label: '1 分钟', value: 60_000 },
]

const RefreshControl: React.FC<RefreshControlProps> = ({
  onRefresh,
  defaultInterval = 10_000,
  loading = false,
}) => {
  const { resolved } = useTheme()
  const [interval, setIntervalValue] = useState(defaultInterval)
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date())

  const doRefresh = useCallback(async () => {
    await onRefresh()
    setLastRefresh(new Date())
  }, [onRefresh])

  useInterval(doRefresh, interval)

  // Re-run when interval changes to refresh immediately on toggle
  useEffect(() => {
    if (interval > 0) {
      void doRefresh()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [interval])

  const menuItems: MenuProps['items'] = INTERVAL_OPTIONS.map((opt) => ({
    key: String(opt.value),
    label: opt.label,
    onClick: () => setIntervalValue(opt.value),
  }))

  return (
    <Space size={8}>
      <Tooltip title={`最后更新: ${dayjs(lastRefresh).format('HH:mm:ss')}`}>
        <Tag
          color={resolved === 'dark' ? 'default' : 'success'}
          style={{ margin: 0, fontFamily: 'JetBrains Mono, monospace' }}
        >
          <ClockCircleOutlined style={{ marginRight: 4 }} />
          {dayjs(lastRefresh).format('HH:mm:ss')}
        </Tag>
      </Tooltip>

      <Button
        size="small"
        icon={<ReloadOutlined spin={loading} />}
        onClick={() => void doRefresh()}
        loading={loading}
      >
        刷新
      </Button>

      <Dropdown menu={{ items: menuItems }} trigger={['click']}>
        <Button size="small">
          <Space size={4}>
            <ClockCircleOutlined />
            {INTERVAL_OPTIONS.find((o) => o.value === interval)?.label ?? '自动'}
            <DownOutlined />
          </Space>
        </Button>
      </Dropdown>
    </Space>
  )
}

export default RefreshControl