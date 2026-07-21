import { useEffect, useState, useCallback } from 'react'
import { Card, Row, Col, Table, Progress, Space, Typography, Button } from 'antd'
import {
  SaveOutlined,
  DatabaseOutlined,
  KeyOutlined,
  CheckCircleOutlined,
  CloudServerOutlined,
  ThunderboltOutlined,
  WarningOutlined,
  FolderOutlined,
} from '@ant-design/icons'
import { useNavigate } from 'react-router-dom'
import type { EChartsOption } from 'echarts'
import type { ClusterMetrics, KVMetrics, AlertInfo, TimeSeriesData, FilerStatus } from '@/types'
import type { SchedulerStatus } from '@/services/api'
import { getClusterMetrics, getKVMetrics, getAlerts, getMetricHistory, getNodes, getFilerStatus, getBalancerStatus } from '@/services/api'
import { connectWebSocket, disconnectWebSocket, type MetricUpdate } from '@/services/websocket'
import { formatBytes, formatPercent, formatUptime, formatNumber } from '@/utils/format'
import { KpiBar, MetricChart, EmptyState, RefreshControl, RealtimeChart, StatusTag } from '@/components/pro'

const { Text, Title } = Typography

function Dashboard() {
  const navigate = useNavigate()
  const [clusterMetrics, setClusterMetrics] = useState<ClusterMetrics | null>(null)
  const [kvMetrics, setKVMetrics] = useState<KVMetrics | null>(null)
  const [filerStatus, setFilerStatus] = useState<FilerStatus | null>(null)
  const [balancerStatus, setBalancerStatus] = useState<SchedulerStatus | null>(null)
  const [alerts, setAlerts] = useState<AlertInfo[]>([])
  const [storageTrend, setStorageTrend] = useState<TimeSeriesData[]>([])
  const [cpuTrend, setCpuTrend] = useState<TimeSeriesData[]>([])
  const [loading, setLoading] = useState(false)

  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const [cluster, kv, alertList, filer, balancer] = await Promise.all([
        getClusterMetrics(),
        getKVMetrics(),
        getAlerts(),
        getFilerStatus(),
        getBalancerStatus(),
      ])
      setClusterMetrics(cluster)
      setKVMetrics(kv)
      setFilerStatus(filer)
      setBalancerStatus(balancer)
      setAlerts(alertList)
    } catch (e) {
      console.error('Failed to load dashboard data:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  const loadHistoryData = useCallback(async () => {
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
  }, [])

  const realtimeFetcher = useCallback(async () => {
    const nodes = await getNodes()
    const online = nodes.filter(n => n.status !== 'offline')
    if (online.length === 0) return { cpu: 0, memory: 0 }
    const sum = online.reduce(
      (acc, n) => ({
        cpu: acc.cpu + (n.cpu_usage || 0),
        memory: acc.memory + (n.mem_usage || 0),
      }),
      { cpu: 0, memory: 0 },
    )
    return {
      cpu: Number((sum.cpu / online.length).toFixed(1)),
      memory: Number((sum.memory / online.length).toFixed(1)),
    }
  }, [])

  useEffect(() => {
    void loadData()
    void loadHistoryData()
    connectWebSocket(onMetricUpdate)
    const interval = setInterval(() => {
      void loadData()
      void loadHistoryData()
    }, 10000)
    return () => {
      clearInterval(interval)
      disconnectWebSocket()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
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

  const storagePercent = clusterMetrics && clusterMetrics.total_storage > 0
    ? (clusterMetrics.used_storage / clusterMetrics.total_storage) * 100
    : 0
  const firingAlerts = alerts.filter(a => a.status === 'firing')
  const recentAlerts = firingAlerts.slice(0, 5)

  const kpiItems = [
    {
      title: '集群节点',
      value: clusterMetrics?.node_count || 0,
      suffix: '个',
      status: 'active',
      icon: <CloudServerOutlined />,
      sparkline: storageTrend.slice(-12).map(d => d.value),
      onClick: () => navigate('/nodes'),
      loading,
      footer: (
        <Space size={4} style={{ color: 'var(--pf-color-success)', fontSize: 12 }}>
          <CheckCircleOutlined /> 全部在线
        </Space>
      ),
    },
    {
      title: 'Volume 数量',
      value: clusterMetrics?.volume_count || 0,
      suffix: '个',
      status: 'active',
      icon: <DatabaseOutlined />,
      onClick: () => navigate('/volumes'),
      loading,
      footer: (
        <Text type="secondary" style={{ fontSize: 12 }}>
          {formatNumber(clusterMetrics?.file_count || 0)} 个文件
        </Text>
      ),
    },
    {
      title: 'KV 会话',
      value: kvMetrics?.session_count || 0,
      suffix: '个',
      status: 'pending',
      icon: <KeyOutlined />,
      onClick: () => navigate('/kv'),
      loading,
      footer: (
        <Text type="secondary" style={{ fontSize: 12 }}>
          {formatNumber(kvMetrics?.block_count || 0)} 个 Block
        </Text>
      ),
    },
    {
      title: '存储使用率',
      value: Number(storagePercent.toFixed(1)),
      precision: 1,
      suffix: '%',
      status: storagePercent > 85 ? 'unreachable' : storagePercent > 70 ? 'draining' : 'active',
      invertDelta: true,
      icon: <ThunderboltOutlined />,
      onClick: () => navigate('/storage-devices'),
      loading,
      footer: (
        <Text type="secondary" style={{ fontSize: 12 }}>
          {formatBytes(clusterMetrics?.used_storage || 0)} / {formatBytes(clusterMetrics?.total_storage || 0)}
        </Text>
      ),
    },
    {
      title: 'Filer 分片',
      value: filerStatus?.shard_count || 0,
      suffix: '个',
      status: (filerStatus?.leader_count || 0) === (filerStatus?.shard_count || 0) ? 'active' : 'draining',
      icon: <DatabaseOutlined />,
      onClick: () => navigate('/shards'),
      loading,
      footer: (
        <Space size={4} style={{ fontSize: 12 }}>
          <CheckCircleOutlined style={{ color: 'var(--pf-color-success)' }} />
          <Text type="secondary">{filerStatus?.leader_count || 0} 个 Leader</Text>
        </Space>
      ),
    },
    {
      title: 'Filer Inode',
      value: filerStatus?.total_inodes || 0,
      suffix: '',
      status: 'active',
      icon: <FolderOutlined />,
      onClick: () => navigate('/filer'),
      loading,
      footer: (
        <Text type="secondary" style={{ fontSize: 12 }}>
          {filerStatus?.total_files || 0} 文件 / {filerStatus?.total_dirs || 0} 目录
        </Text>
      ),
    },
    {
      title: '分片均衡',
      value: balancerStatus?.is_running ? 1 : 0,
      suffix: '',
      status: balancerStatus?.is_running ? 'active' : 'draining',
      icon: <ThunderboltOutlined />,
      onClick: () => navigate('/shard-balancing'),
      loading,
      footer: (
        <Text type="secondary" style={{ fontSize: 12 }}>
          {balancerStatus?.is_running ? '运行中' : '已停止'} · 迁移 {balancerStatus?.successful_migrations || 0}/{balancerStatus?.total_migrations || 0}
        </Text>
      ),
    },
  ]

  const storageChartOption: EChartsOption = {
    tooltip: { trigger: 'axis', formatter: '{b}<br/>存储使用率: {c}%' },
    xAxis: {
      type: 'category',
      data: storageTrend.map(d => {
        const date = new Date(d.time)
        return `${date.getHours()}:00`
      }),
    },
    yAxis: { type: 'value', axisLabel: { formatter: '{value}%' } },
    series: [
      {
        name: '存储使用率',
        type: 'line',
        smooth: true,
        data: storageTrend.map(d => d.value),
        areaStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
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
  }

  const cpuChartOption: EChartsOption = {
    tooltip: { trigger: 'axis', formatter: '{b}<br/>CPU使用率: {c}%' },
    xAxis: {
      type: 'category',
      data: cpuTrend.map(d => {
        const date = new Date(d.time)
        return `${date.getHours()}:00`
      }),
    },
    yAxis: { type: 'value', axisLabel: { formatter: '{value}%' } },
    series: [
      {
        name: 'CPU使用率',
        type: 'line',
        smooth: true,
        data: cpuTrend.map(d => d.value),
        areaStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(22, 119, 255, 0.3)' },
              { offset: 1, color: 'rgba(22, 119, 255, 0.05)' },
            ],
          },
        },
        lineStyle: { color: '#1677ff', width: 3 },
        itemStyle: { color: '#1677ff' },
      },
    ],
  }

  const alertColumns = [
    { title: '告警名称', dataIndex: 'name', key: 'name' },
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
      render: (severity: string) => <StatusTag kind="alert" status={severity} pulse={severity === 'critical'} />,
    },
    { title: '来源', dataIndex: 'source', key: 'source' },
    { title: '消息', dataIndex: 'message', key: 'message' },
    {
      title: '时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (time: string) => new Date(time).toLocaleString(),
    },
  ]

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <Title level={4} style={{ margin: 0 }}>集群总览</Title>
          <Text type="secondary">PowerFS 集群实时状态与关键指标</Text>
        </div>
        <RefreshControl onRefresh={loadData} loading={loading} />
      </div>

      <div style={{ marginBottom: 24 }}>
        <KpiBar items={kpiItems} />
      </div>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} lg={12}>
          <Card
            title="存储使用趋势"
            style={{ borderRadius: 12 }}
            styles={{ body: { padding: 20 } }}
          >
            <MetricChart option={storageChartOption} height={300} loading={loading} />
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card
            title="CPU 使用趋势"
            style={{ borderRadius: 12 }}
            styles={{ body: { padding: 20 } }}
          >
            <MetricChart option={cpuChartOption} height={300} loading={loading} />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24}>
          <RealtimeChart
            title={<Space><ThunderboltOutlined />实时节点性能</Space>}
            fetcher={realtimeFetcher}
            interval={5000}
            maxPoints={60}
            height={240}
            yAxis={{ min: 0, max: 100, unit: '%' }}
            series={[
              { key: 'cpu', name: 'CPU 使用率', color: '#1677ff' },
              { key: 'memory', name: '内存使用率', color: '#722ed1' },
            ]}
          />
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} lg={12}>
          <Card
            title={<Space><SaveOutlined />集群状态</Space>}
            style={{ borderRadius: 12 }}
            styles={{ body: { padding: 20 } }}
          >
            <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">运行时间</Text>
                <Text strong className="tabular-nums">{formatUptime(clusterMetrics?.uptime || 0)}</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">Leader 状态</Text>
                {clusterMetrics?.is_leader ? (
                  <StatusTag kind="node" status="active" label="Leader" pulse />
                ) : (
                  <StatusTag kind="node" status="draining" label="Follower" />
                )}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">Raft Term</Text>
                <Text strong className="tabular-nums">{clusterMetrics?.raft_term || 0}</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">Collection 数量</Text>
                <Text strong>{clusterMetrics?.collection_count || 0} 个</Text>
              </div>
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <Text type="secondary">存储使用</Text>
                  <Text strong>{formatPercent(storagePercent)}</Text>
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
        <Col xs={24} lg={12}>
          <Card
            title={<Space><KeyOutlined />KV 缓存统计</Space>}
            style={{ borderRadius: 12 }}
            styles={{ body: { padding: 20 } }}
          >
            <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">内存使用</Text>
                <Text strong className="tabular-nums">{formatBytes(kvMetrics?.memory_used || 0)}</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">命中率</Text>
                <Text
                  strong
                  className="tabular-nums"
                  style={{ color: (kvMetrics?.hit_ratio ?? 0) >= 90 ? 'var(--pf-color-success)' : 'var(--pf-color-warning)' }}
                >
                  {formatPercent(kvMetrics?.hit_ratio || 0)}
                </Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">驱逐次数</Text>
                <Text strong className="tabular-nums">{kvMetrics?.eviction_count || 0} 次</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">平均延迟</Text>
                <Text strong className="tabular-nums">{(kvMetrics?.avg_latency || 0).toFixed(2)} ms</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">总请求数</Text>
                <Text strong className="tabular-nums">
                  {formatNumber((kvMetrics?.put_count || 0) + (kvMetrics?.get_count || 0))} 次
                </Text>
              </div>
            </Space>
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card
            title={<Space><DatabaseOutlined />Filer 状态</Space>}
            extra={<Button type="link" onClick={() => navigate('/filer')}>查看全部</Button>}
            style={{ borderRadius: 12 }}
            styles={{ body: { padding: 20 } }}
          >
            <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">分片数量</Text>
                <Text strong className="tabular-nums">{filerStatus?.shard_count || 0} 个</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">Leader 状态</Text>
                {(filerStatus?.leader_count || 0) === (filerStatus?.shard_count || 0) ? (
                  <StatusTag kind="node" status="active" label="全部正常" pulse />
                ) : (
                  <StatusTag kind="node" status="draining" label={`${filerStatus?.leader_count || 0}/${filerStatus?.shard_count || 0}`} />
                )}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">Inode 总数</Text>
                <Text strong className="tabular-nums">{formatNumber(filerStatus?.total_inodes || 0)}</Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text type="secondary">文件数</Text>
                <Text strong className="tabular-nums">{formatNumber(filerStatus?.total_files || 0)}</Text>
              </div>
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <Text type="secondary">Bucket 列表</Text>
                  <Text strong>{filerStatus?.buckets.length || 0} 个</Text>
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                  {filerStatus?.buckets.length ? (
                    filerStatus.buckets.slice(0, 5).map((bucket) => (
                      <span
                        key={bucket}
                        style={{
                          padding: '4px 12px',
                          background: 'rgba(22, 119, 255, 0.1)',
                          color: '#1677ff',
                          borderRadius: 4,
                          fontSize: 12,
                        }}
                      >
                        {bucket}
                      </span>
                    ))
                  ) : (
                    <Text type="secondary" style={{ fontSize: 12 }}>暂无 Bucket</Text>
                  )}
                  {filerStatus?.buckets.length && filerStatus.buckets.length > 5 && (
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      +{filerStatus.buckets.length - 5} 更多
                    </Text>
                  )}
                </div>
              </div>
            </Space>
          </Card>
        </Col>
      </Row>

      <Card
        title={<Space><WarningOutlined />最近告警</Space>}
        extra={
          <Button type="link" onClick={() => navigate('/alerts')}>查看全部</Button>
        }
        style={{ borderRadius: 12 }}
        styles={{ body: { padding: 20 } }}
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
          <EmptyState
            title="暂无告警"
            description="集群当前运行正常"
            icon={<CheckCircleOutlined style={{ color: 'var(--pf-color-success)' }} />}
          />
        )}
      </Card>
    </div>
  )
}

export default Dashboard