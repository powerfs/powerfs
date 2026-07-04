import { useEffect, useState } from 'react'
import { Card, Row, Col, Statistic, Table, Tag, Progress, Space } from 'antd'
import {
  SaveOutlined,
  DatabaseOutlined,
  KeyOutlined,
  CheckCircleOutlined,
  DatabaseOutlined as HardDriveOutlined,
} from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import type { ClusterMetrics, KVMetrics, AlertInfo, TimeSeriesData } from '@/types'
import { getClusterMetrics, getKVMetrics, getAlerts, getMetricHistory } from '@/services/api'
import { connectWebSocket, disconnectWebSocket, type MetricUpdate } from '@/services/websocket'
import { formatBytes, formatPercent, formatUptime, formatNumber } from '@/utils/format'

function Dashboard() {
  const [clusterMetrics, setClusterMetrics] = useState<ClusterMetrics | null>(null)
  const [kvMetrics, setKVMetrics] = useState<KVMetrics | null>(null)
  const [alerts, setAlerts] = useState<AlertInfo[]>([])
  const [storageTrend, setStorageTrend] = useState<TimeSeriesData[]>([])
  const [cpuTrend, setCpuTrend] = useState<TimeSeriesData[]>([])

  useEffect(() => {
    loadData()
    loadHistoryData()
    
    connectWebSocket(onMetricUpdate)

    const interval = setInterval(loadData, 10000)

    return () => {
      clearInterval(interval)
      disconnectWebSocket()
    }
  }, [])

  const onMetricUpdate = (data: MetricUpdate) => {
    if (data.type === 'metric_update') {
      if (data.source === 'cluster') {
        setClusterMetrics(prev => ({ ...prev, ...data.payload } as ClusterMetrics))
      } else if (data.source === 'kv') {
        setKVMetrics(prev => ({ ...prev, ...data.payload } as KVMetrics))
      }
    }
  }

  const loadHistoryData = async () => {
    try {
      const [storageData, cpuData] = await Promise.all([
        getMetricHistory('powerfs_node_disk_usage'),
        getMetricHistory('powerfs_node_cpu_usage'),
      ])
      setStorageTrend(storageData)
      setCpuTrend(cpuData)
    } catch (e) {
      console.error('Failed to load history data:', e)
    }
  }

  const loadData = async () => {
    const [cluster, kv, alertList] = await Promise.all([
      getClusterMetrics(),
      getKVMetrics(),
      getAlerts(),
    ])
    setClusterMetrics(cluster)
    setKVMetrics(kv)
    setAlerts(alertList)
  }

  const storagePercent = clusterMetrics
    ? (clusterMetrics.used_storage / clusterMetrics.total_storage) * 100
    : 0

  const recentAlerts = alerts.filter(a => a.status === 'firing').slice(0, 5)

  const alertColumns = [
    {
      title: '告警名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
      render: (severity: string) => (
        <Tag color={severity === 'critical' ? 'red' : severity === 'warning' ? 'orange' : 'blue'}>
          {severity === 'critical' ? '严重' : severity === 'warning' ? '警告' : '信息'}
        </Tag>
      ),
    },
    {
      title: '来源',
      dataIndex: 'source',
      key: 'source',
    },
    {
      title: '消息',
      dataIndex: 'message',
      key: 'message',
    },
    {
      title: '时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (time: string) => new Date(time).toLocaleString(),
    },
  ]

  return (
    <div>
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#e6f7ff', padding: 8, borderRadius: 8 }}>
                  <SaveOutlined style={{ fontSize: 24, color: '#1890ff' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>集群节点</span>
              </div>
              <Statistic
                value={clusterMetrics?.node_count || 0}
                suffix="个"
                valueStyle={{ fontSize: 32, fontWeight: 'bold', color: '#1890ff' }}
              />
              <div style={{ display: 'flex', alignItems: 'center', gap: 4, color: '#52c41a' }}>
                <CheckCircleOutlined />
                <span style={{ fontSize: 12 }}>全部在线</span>
              </div>
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#f6ffed', padding: 8, borderRadius: 8 }}>
                  <DatabaseOutlined style={{ fontSize: 24, color: '#52c41a' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>Volume数量</span>
              </div>
              <Statistic
                value={clusterMetrics?.volume_count || 0}
                suffix="个"
                valueStyle={{ fontSize: 32, fontWeight: 'bold', color: '#52c41a' }}
              />
              <div style={{ fontSize: 12, color: '#8c8c8c' }}>
                {formatNumber(clusterMetrics?.file_count || 0)} 个文件
              </div>
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#fff7e6', padding: 8, borderRadius: 8 }}>
                  <KeyOutlined style={{ fontSize: 24, color: '#fa8c16' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>KV会话</span>
              </div>
              <Statistic
                value={kvMetrics?.session_count || 0}
                suffix="个"
                valueStyle={{ fontSize: 32, fontWeight: 'bold', color: '#fa8c16' }}
              />
              <div style={{ fontSize: 12, color: '#8c8c8c' }}>
                {formatNumber(kvMetrics?.block_count || 0)} 个Block
              </div>
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#fff0f6', padding: 8, borderRadius: 8 }}>
                  <HardDriveOutlined style={{ fontSize: 24, color: '#eb2f96' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>存储使用</span>
              </div>
              <Statistic
                value={formatPercent(storagePercent)}
                valueStyle={{ fontSize: 32, fontWeight: 'bold', color: '#eb2f96' }}
              />
              <div style={{ fontSize: 12, color: '#8c8c8c' }}>
                {formatBytes(clusterMetrics?.used_storage || 0)} / {formatBytes(clusterMetrics?.total_storage || 0)}
              </div>
            </Space>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={12}>
          <Card
            title="存储使用趋势"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <ReactECharts
              option={{
                tooltip: {
                  trigger: 'axis',
                  formatter: '{b}<br/>存储使用率: {c}%',
                },
                grid: {
                  left: '3%',
                  right: '4%',
                  bottom: '3%',
                  containLabel: true,
                },
                xAxis: {
                  type: 'category',
                  data: storageTrend.map(d => {
                    const date = new Date(d.time)
                    return `${date.getHours()}:00`
                  }),
                  axisLine: { lineStyle: { color: '#d9d9d9' } },
                  axisLabel: { color: '#8c8c8c' },
                },
                yAxis: {
                  type: 'value',
                  axisLine: { show: false },
                  axisTick: { show: false },
                  splitLine: { lineStyle: { color: '#f0f0f0' } },
                  axisLabel: { color: '#8c8c8c', formatter: '{value}%' },
                },
                series: [
                  {
                    name: '存储使用率',
                    type: 'line',
                    smooth: true,
                    data: storageTrend.map(d => d.value),
                    areaStyle: {
                      color: {
                        type: 'linear',
                        x: 0,
                        y: 0,
                        x2: 0,
                        y2: 1,
                        colorStops: [
                          { offset: 0, color: 'rgba(235, 47, 150, 0.3)' },
                          { offset: 1, color: 'rgba(235, 47, 150, 0.05)' },
                        ],
                      },
                    },
                    lineStyle: { color: '#eb2f96', width: 3 },
                    itemStyle: { color: '#eb2f96' },
                  },
                ],
              }}
              style={{ height: 300 }}
            />
          </Card>
        </Col>
        <Col span={12}>
          <Card
            title="CPU使用趋势"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <ReactECharts
              option={{
                tooltip: {
                  trigger: 'axis',
                  formatter: '{b}<br/>CPU使用率: {c}%',
                },
                grid: {
                  left: '3%',
                  right: '4%',
                  bottom: '3%',
                  containLabel: true,
                },
                xAxis: {
                  type: 'category',
                  data: cpuTrend.map(d => {
                    const date = new Date(d.time)
                    return `${date.getHours()}:00`
                  }),
                  axisLine: { lineStyle: { color: '#d9d9d9' } },
                  axisLabel: { color: '#8c8c8c' },
                },
                yAxis: {
                  type: 'value',
                  axisLine: { show: false },
                  axisTick: { show: false },
                  splitLine: { lineStyle: { color: '#f0f0f0' } },
                  axisLabel: { color: '#8c8c8c', formatter: '{value}%' },
                },
                series: [
                  {
                    name: 'CPU使用率',
                    type: 'line',
                    smooth: true,
                    data: cpuTrend.map(d => d.value),
                    areaStyle: {
                      color: {
                        type: 'linear',
                        x: 0,
                        y: 0,
                        x2: 0,
                        y2: 1,
                        colorStops: [
                          { offset: 0, color: 'rgba(24, 144, 255, 0.3)' },
                          { offset: 1, color: 'rgba(24, 144, 255, 0.05)' },
                        ],
                      },
                    },
                    lineStyle: { color: '#1890ff', width: 3 },
                    itemStyle: { color: '#1890ff' },
                  },
                ],
              }}
              style={{ height: 300 }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col span={12}>
          <Card
            title="集群状态"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>运行时间</span>
                <span style={{ fontWeight: 500 }}>{formatUptime(clusterMetrics?.uptime || 0)}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>Leader状态</span>
                <Tag color={clusterMetrics?.is_leader ? 'green' : 'red'}>
                  {clusterMetrics?.is_leader ? 'Leader' : 'Follower'}
                </Tag>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>Raft Term</span>
                <span style={{ fontWeight: 500 }}>{clusterMetrics?.raft_term || 0}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>Collection数量</span>
                <span style={{ fontWeight: 500 }}>{clusterMetrics?.collection_count || 0} 个</span>
              </div>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span style={{ color: '#8c8c8c' }}>存储使用</span>
                  <span style={{ fontWeight: 500 }}>{formatPercent(storagePercent)}</span>
                </div>
                <Progress
                  percent={storagePercent}
                  strokeColor={{
                    '0%': '#52c41a',
                    '70%': '#faad14',
                    '100%': '#f5222d',
                  }}
                  showInfo={false}
                />
              </div>
            </Space>
          </Card>
        </Col>
        <Col span={12}>
          <Card
            title="KV缓存统计"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>内存使用</span>
                <span style={{ fontWeight: 500 }}>{formatBytes(kvMetrics?.memory_used || 0)}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>命中率</span>
                <span style={{ fontWeight: 500, color: kvMetrics?.hit_ratio && kvMetrics.hit_ratio >= 90 ? '#52c41a' : '#faad14' }}>
                  {formatPercent(kvMetrics?.hit_ratio || 0)}
                </span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>驱逐次数</span>
                <span style={{ fontWeight: 500 }}>{kvMetrics?.eviction_count || 0} 次</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>平均延迟</span>
                <span style={{ fontWeight: 500 }}>{(kvMetrics?.avg_latency || 0).toFixed(2)} ms</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>总请求数</span>
                <span style={{ fontWeight: 500 }}>{formatNumber((kvMetrics?.put_count || 0) + (kvMetrics?.get_count || 0))} 次</span>
              </div>
            </Space>
          </Card>
        </Col>
      </Row>

      <Card
        title="最近告警"
        style={{ borderRadius: 12, marginTop: 16 }}
        bodyStyle={{ padding: '20px' }}
      >
        {recentAlerts.length > 0 ? (
          <Table
            columns={alertColumns}
            dataSource={recentAlerts}
            rowKey="id"
            pagination={false}
            size="small"
          />
        ) : (
          <div style={{ textAlign: 'center', padding: '40px 0', color: '#8c8c8c' }}>
            <Space direction="vertical" align="center">
              <CheckCircleOutlined style={{ fontSize: 48, color: '#52c41a' }} />
              <span>暂无告警</span>
            </Space>
          </div>
        )}
      </Card>
    </div>
  )
}

export default Dashboard