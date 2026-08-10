import { useEffect, useState, useMemo } from 'react'
import { Card, Select, Space, Typography, Empty, Spin, Row, Col, Statistic, Tag, Tooltip } from 'antd'
import ReactECharts from 'echarts-for-react'
import { getVolumes, getCapacityHistory, getCapacityProjection, getClusterDiskUsageBreakdown, getMetricHistory, type CapacityHistoryResponse, type CapacityProjectionResponse, type NodeDiskUsageSeries } from '@/services/api'
import type { VolumeInfo } from '@/types'
import dayjs from 'dayjs'

const { Title, Text } = Typography
const { Option } = Select

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const k = 1024
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${units[i]}`
}

export default function CapacityPlanning() {
  const [volumes, setVolumes] = useState<VolumeInfo[]>([])
  const [selectedVolume, setSelectedVolume] = useState<number | null>(null)
  const [rangeMinutes, setRangeMinutes] = useState<number>(1440)
  const [history, setHistory] = useState<CapacityHistoryResponse | null>(null)
  const [projection, setProjection] = useState<CapacityProjectionResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [projectHours, setProjectHours] = useState<number>(24)
  // Cluster-wide disk usage trend (P3: GET /api/metrics/cluster-disk-usage)
  const [clusterSeries, setClusterSeries] = useState<NodeDiskUsageSeries[]>([])
  const [clusterAvgTrend, setClusterAvgTrend] = useState<{ time: string; value: number }[]>([])
  const [clusterTrendLoading, setClusterTrendLoading] = useState(false)
  // Reuse rangeMinutes for cluster trend lookback, but clamp to 7d max (matches backend limit).
  const clusterLookback = Math.min(rangeMinutes, 10080)

  useEffect(() => {
    loadVolumes()
  }, [])

  const loadVolumes = async () => {
    try {
      const data = await getVolumes()
      setVolumes(data)
      if (data.length > 0 && !selectedVolume) {
        setSelectedVolume(data[0].id)
      }
    } catch (e) {
      console.error('Failed to load volumes', e)
    }
  }

  useEffect(() => {
    if (selectedVolume !== null) {
      loadData()
    }
  }, [selectedVolume, rangeMinutes, projectHours])

  // Cluster-wide trend is independent of selected volume — only depends on the lookback window.
  useEffect(() => {
    loadClusterTrend()
  }, [clusterLookback])

  const loadData = async () => {
    if (selectedVolume === null) return
    setLoading(true)
    try {
      const [hist, proj] = await Promise.all([
        getCapacityHistory(selectedVolume, rangeMinutes),
        getCapacityProjection(selectedVolume, projectHours),
      ])
      setHistory(hist)
      setProjection(proj)
    } catch (e) {
      console.error('Failed to load capacity data', e)
    } finally {
      setLoading(false)
    }
  }

  const loadClusterTrend = async () => {
    setClusterTrendLoading(true)
    try {
      const [series, avg] = await Promise.all([
        getClusterDiskUsageBreakdown(clusterLookback),
        getMetricHistory('cluster_disk_usage', clusterLookback),
      ])
      setClusterSeries(series)
      setClusterAvgTrend(avg)
    } catch (e) {
      console.error('Failed to load cluster disk usage trend', e)
      setClusterSeries([])
      setClusterAvgTrend([])
    } finally {
      setClusterTrendLoading(false)
    }
  }

  const renderChart = () => {
    if (!history || history.data_points.length === 0) {
      return <Empty description="暂无历史数据。采样器每 60 秒记录一次容量。" />
    }

    const option = {
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const p = params[0]
          const ts = dayjs(p.value[0] * 1000).format('YYYY-MM-DD HH:mm')
          return `${ts}<br/>使用量: ${formatBytes(p.value[1])}`
        },
      },
      grid: { left: 80, right: 30, top: 30, bottom: 60 },
      xAxis: {
        type: 'time',
        axisLabel: {
          formatter: (value: number) => dayjs(value * 1000).format('MM-DD HH:mm'),
        },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: (value: number) => formatBytes(value),
        },
      },
      series: [
        {
          name: '已用容量',
          type: 'line',
          smooth: true,
          data: history.data_points.map((p) => [p.timestamp, p.value]),
          areaStyle: {
            opacity: 0.3,
          },
          lineStyle: { color: '#1677ff' },
          itemStyle: { color: '#1677ff' },
        },
      ],
    }

    return <ReactECharts option={option} style={{ height: 350 }} />
  }

  // ─── Cluster-wide disk usage trend chart (multi-series + avg line) ───
  // Builds a single ECharts option combining:
  //   * one line per node (from clusterSeries, each series is its own color)
  //   * a bold black avg line (from clusterAvgTrend)
  const clusterChartOption = useMemo(() => {
    if (clusterSeries.length === 0 && clusterAvgTrend.length === 0) {
      return null
    }
    // Color palette (10 distinct colors, will cycle if >10 nodes).
    const palette = [
      '#1677ff', '#52c41a', '#faad14', '#eb2f96', '#722ed1',
      '#13c2c2', '#fa541c', '#a0d911', '#2f54eb', '#f5222d',
    ]
    const nodeSeries = clusterSeries.map((s, idx) => ({
      name: s.node_id,
      type: 'line' as const,
      smooth: true,
      symbol: 'circle',
      symbolSize: 4,
      showSymbol: false,
      lineStyle: { width: 1.5, color: palette[idx % palette.length], opacity: 0.7 },
      itemStyle: { color: palette[idx % palette.length] },
      data: s.points.map((p) => [dayjs(p.time).valueOf(), Number(p.value.toFixed(2))]),
    }))
    const avgSeries = {
      name: '集群平均',
      type: 'line' as const,
      smooth: true,
      symbol: 'none',
      showSymbol: false,
      lineStyle: { width: 3, color: '#000', type: 'dashed' as const },
      itemStyle: { color: '#000' },
      z: 10,
      data: clusterAvgTrend.map((p) => [dayjs(p.time).valueOf(), Number(p.value.toFixed(2))]),
    }
    return {
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          if (!Array.isArray(params) || params.length === 0) return ''
          const ts = dayjs(params[0].value[0]).format('YYYY-MM-DD HH:mm')
          const lines = params
            .map((p: any) => `${p.marker} ${p.seriesName}: ${Number(p.value[1]).toFixed(2)}%`)
            .join('<br/>')
          return `${ts}<br/>${lines}`
        },
      },
      legend: {
        type: 'scroll' as const,
        bottom: 0,
        textStyle: { fontSize: 11 },
      },
      grid: { left: 50, right: 30, top: 20, bottom: 50 },
      xAxis: {
        type: 'time',
        axisLabel: {
          formatter: (value: number) => dayjs(value).format('MM-DD HH:mm'),
        },
      },
      yAxis: {
        type: 'value',
        min: 0,
        max: 100,
        axisLabel: { formatter: '{value}%' },
      },
      series: [...nodeSeries, avgSeries],
    }
  }, [clusterSeries, clusterAvgTrend])

  // Cluster-level KPIs derived from latest points.
  const clusterKpis = useMemo(() => {
    if (clusterAvgTrend.length === 0) {
      return { latestAvg: null, maxAvg: null, nodeCount: 0, hottestNode: null, hottestNodeValue: null }
    }
    const latestAvg = clusterAvgTrend[clusterAvgTrend.length - 1].value
    const maxAvg = Math.max(...clusterAvgTrend.map((p) => p.value))
    const nodeLatest = clusterSeries
      .map((s) => {
        const last = s.points[s.points.length - 1]
        return last ? { node_id: s.node_id, value: last.value } : null
      })
      .filter(Boolean) as { node_id: string; value: number }[]
    const hottest = nodeLatest.reduce(
      (acc, n) => (n.value > acc.value ? n : acc),
      nodeLatest[0] ?? { node_id: '-', value: 0 },
    )
    return {
      latestAvg,
      maxAvg,
      nodeCount: nodeLatest.length,
      hottestNode: hottest.node_id,
      hottestNodeValue: hottest.value,
    }
  }, [clusterSeries, clusterAvgTrend])

  const renderClusterChart = () => {
    if (clusterSeries.length === 0 && clusterAvgTrend.length === 0) {
      return (
        <Empty
          description={
            <span>
              暂无集群磁盘使用率数据
              <br />
              <Text type="secondary" style={{ fontSize: 12 }}>
                采样器每 60 秒记录一次各节点 disk_usage；新部署需要几分钟积累数据。
              </Text>
            </span>
          }
        />
      )
    }
    if (!clusterChartOption) return <Empty description="数据解析中..." />
    return <ReactECharts option={clusterChartOption} style={{ height: 380 }} />
  }

  return (
    <div>
      <Title level={3}>容量规划</Title>
      <Card style={{ marginBottom: 16 }}>
        <Space>
          <span>选择 Volume:</span>
          <Select
            value={selectedVolume ?? undefined}
            onChange={(v) => setSelectedVolume(v)}
            style={{ width: 240 }}
            showSearch
            optionFilterProp="label"
          >
            {volumes.map((v) => (
              <Option key={v.id} value={v.id} label={`Volume ${v.id} (${v.collection})`}>
                Volume {v.id} - {v.collection} ({formatBytes(v.used)})
              </Option>
            ))}
          </Select>

          <span>时间范围:</span>
          <Select
            value={rangeMinutes}
            onChange={(v) => setRangeMinutes(v)}
            style={{ width: 120 }}
          >
            <Option value={60}>1 小时</Option>
            <Option value={360}>6 小时</Option>
            <Option value={1440}>24 小时</Option>
            <Option value={4320}>3 天</Option>
            <Option value={10080}>7 天</Option>
          </Select>

          <span>预测时长:</span>
          <Select
            value={projectHours}
            onChange={(v) => setProjectHours(v)}
            style={{ width: 120 }}
          >
            <Option value={6}>6 小时</Option>
            <Option value={24}>24 小时</Option>
            <Option value={72}>3 天</Option>
            <Option value={168}>7 天</Option>
            <Option value={720}>30 天</Option>
          </Select>
        </Space>
      </Card>

      <Spin spinning={loading}>
        <Row gutter={16}>
          <Col span={16}>
            <Card title="容量历史趋势">
              {renderChart()}
            </Card>
          </Col>

          <Col span={8}>
            <Card title="容量预测">
              {projection ? (
                <div>
                  <Row gutter={16} style={{ marginBottom: 16 }}>
                    <Col span={24}>
                      <Statistic
                        title="当前使用量"
                        value={formatBytes(projection.current_bytes)}
                        valueStyle={{ color: '#1677ff' }}
                      />
                    </Col>
                  </Row>

                  {projection.projected_bytes !== null ? (
                    <Row gutter={16} style={{ marginBottom: 16 }}>
                      <Col span={24}>
                        <Statistic
                          title={`${projectHours} 小时后预测`}
                          value={formatBytes(projection.projected_bytes)}
                          valueStyle={{
                            color: projection.projected_bytes > projection.current_bytes ? '#faad14' : '#52c41a',
                          }}
                        />
                      </Col>
                    </Row>
                  ) : (
                    <Tag color="orange">数据不足，无法预测（至少需要 2 个采样点）</Tag>
                  )}

                  {projection.growth_rate_bytes_per_hour !== null && (
                    <Row gutter={16}>
                      <Col span={24}>
                        <Statistic
                          title="增长速率"
                          value={`${formatBytes(projection.growth_rate_bytes_per_hour)}/小时`}
                          valueStyle={{ color: '#722ed1' }}
                        />
                      </Col>
                    </Row>
                  )}

                  {projection.projected_bytes !== null && projection.growth_rate_bytes_per_hour !== null && projection.growth_rate_bytes_per_hour > 0 && (
                    <div style={{ marginTop: 16, padding: 12, background: '#fffbe6', borderRadius: 4 }}>
                      <Tag color="warning">容量警告</Tag>
                      <div style={{ marginTop: 8, fontSize: 12 }}>
                        按当前速率，{projectHours} 小时后将使用 <strong>{formatBytes(projection.projected_bytes)}</strong>
                      </div>
                      <div style={{ fontSize: 12 }}>
                        预计填满时间: {estimateFullTime(projection.current_bytes, projection.growth_rate_bytes_per_hour, volumes.find(v => v.id === selectedVolume)?.size ?? 0)}
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <Empty description="选择一个 Volume 以查看预测" />
              )}
            </Card>
          </Col>
        </Row>
      </Spin>

      {/* ═══════════ P3: Cluster-wide disk usage trend (multi-node) ═══════════ */}
      <Spin spinning={clusterTrendLoading}>
        <Row gutter={16} style={{ marginTop: 16 }}>
          <Col span={18}>
            <Card
              title={
                <Space>
                  <span>集群磁盘使用率趋势</span>
                  <Tag color="blue" style={{ fontSize: 11 }}>
                    {clusterSeries.length} 个节点 · {clusterLookback >= 1440 ? `${Math.round(clusterLookback / 1440)} 天` : `${Math.round(clusterLookback / 60)} 小时`}
                  </Tag>
                  <Tooltip title="数据来源: TimeSeriesStore.get_per_node_disk_usage (60s 采样)，曲线为各节点 disk_usage 百分比，黑色虚线为集群平均值">
                    <Text type="secondary" style={{ fontSize: 11 }}>?</Text>
                  </Tooltip>
                </Space>
              }
              extra={
                <Tooltip title="刷新">
                  <Text
                    onClick={loadClusterTrend}
                    style={{ cursor: 'pointer', color: 'var(--pf-color-primary)', fontSize: 13 }}
                  >
                    刷新
                  </Text>
                </Tooltip>
              }
            >
              {renderClusterChart()}
            </Card>
          </Col>

          <Col span={6}>
            <Card title="集群 KPI" style={{ height: '100%' }}>
              {clusterKpis.latestAvg === null ? (
                <Empty description="暂无数据" />
              ) : (
                <div>
                  <Row gutter={16} style={{ marginBottom: 16 }}>
                    <Col span={24}>
                      <Statistic
                        title="集群平均磁盘使用率"
                        value={clusterKpis.latestAvg}
                        precision={2}
                        suffix="%"
                        valueStyle={{
                          color:
                            clusterKpis.latestAvg > 90 ? '#ff4d4f'
                              : clusterKpis.latestAvg > 80 ? '#faad14'
                              : '#52c41a',
                        }}
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} style={{ marginBottom: 16 }}>
                    <Col span={24}>
                      <Statistic
                        title="区间峰值平均"
                        value={clusterKpis.maxAvg ?? 0}
                        precision={2}
                        suffix="%"
                        valueStyle={{ color: '#722ed1' }}
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} style={{ marginBottom: 16 }}>
                    <Col span={12}>
                      <Statistic
                        title="采样节点数"
                        value={clusterKpis.nodeCount}
                        suffix="个"
                      />
                    </Col>
                    <Col span={12}>
                      <Statistic
                        title="热点节点"
                        value={clusterKpis.hottestNode ?? '-'}
                        valueStyle={{ fontSize: 14 }}
                      />
                    </Col>
                  </Row>
                  <Row gutter={16}>
                    <Col span={24}>
                      <Statistic
                        title="热点节点使用率"
                        value={clusterKpis.hottestNodeValue ?? 0}
                        precision={2}
                        suffix="%"
                        valueStyle={{
                          color:
                            (clusterKpis.hottestNodeValue ?? 0) > 90 ? '#ff4d4f'
                              : (clusterKpis.hottestNodeValue ?? 0) > 80 ? '#faad14'
                              : '#52c41a',
                        }}
                      />
                    </Col>
                  </Row>
                </div>
              )}
            </Card>
          </Col>
        </Row>
      </Spin>
    </div>
  )
}

function estimateFullTime(current: number, rate: number, totalSize: number): string {
  if (rate <= 0 || totalSize <= 0) return '未知'
  const remaining = totalSize - current
  if (remaining <= 0) return '已满'
  const hours = remaining / rate
  if (hours < 1) return `${Math.round(hours * 60)} 分钟`
  if (hours < 24) return `${hours.toFixed(1)} 小时`
  const days = hours / 24
  return `${days.toFixed(1)} 天`
}
