import { useEffect, useState } from 'react'
import { Card, Select, Space, Typography, Empty, Spin, Row, Col, Statistic, Tag } from 'antd'
import ReactECharts from 'echarts-for-react'
import { getVolumes, getCapacityHistory, getCapacityProjection, type CapacityHistoryResponse, type CapacityProjectionResponse } from '@/services/api'
import type { VolumeInfo } from '@/types'
import dayjs from 'dayjs'

const { Title } = Typography
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
