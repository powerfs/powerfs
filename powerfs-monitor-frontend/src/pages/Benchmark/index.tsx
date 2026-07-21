import { useState, useEffect, useCallback } from 'react'
import { Card, Row, Col, Table, Button, Space, Typography, Tag, Divider, Modal, Descriptions } from 'antd'
import {
  PlayCircleOutlined,
  RestOutlined,
  DatabaseOutlined,
  FileTextOutlined,
  FolderOpenOutlined,
  RocketOutlined,
  ClockCircleOutlined,
  ApiOutlined,
  EyeOutlined,
} from '@ant-design/icons'
import type { EChartsOption } from 'echarts'
import type { BenchmarkResult, BenchmarkReport } from '@/types'
import { getBenchmarkResults, runBenchmark, getBenchmarkReportById } from '@/services/api'
import { MetricChart, StatCard, RefreshControl } from '@/components/pro'

const { Text, Title } = Typography

function Benchmark() {
  const [results, setResults] = useState<BenchmarkResult[]>([])
  const [loading, setLoading] = useState(false)
  const [runningType, setRunningType] = useState<string | null>(null)
  const [selectedTab, setSelectedTab] = useState<'kv' | 'metadata' | 'fs' | 's3'>('kv')
  const [detailModalVisible, setDetailModalVisible] = useState(false)
  const [detailData, setDetailData] = useState<BenchmarkResult | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const data = await getBenchmarkResults()
      setResults(data)
    } catch (e) {
      console.error('Failed to load benchmark results:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadData()
  }, [loadData])

  const handleRunBenchmark = async (type: 'kv' | 'metadata' | 'fs' | 's3') => {
    setRunningType(type)
    try {
      const result = await runBenchmark(type)
      setResults(prev => [result, ...prev])
      setSelectedTab(type)
    } catch (e) {
      console.error('Failed to run benchmark:', e)
    } finally {
      setRunningType(null)
    }
  }

  const handleViewDetail = async (record: BenchmarkResult) => {
    setDetailLoading(true)
    try {
      const data = await getBenchmarkReportById(record.id)
      setDetailData(data)
      setDetailModalVisible(true)
    } catch (e) {
      console.error('Failed to load benchmark detail:', e)
    } finally {
      setDetailLoading(false)
    }
  }

  const getCurrentReport = (): BenchmarkReport | undefined => {
    return results.find(r => r.type === selectedTab && r.status === 'completed')?.result
  }

  const currentReport = getCurrentReport()

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'kv':
        return <DatabaseOutlined />
      case 'metadata':
        return <FileTextOutlined />
      case 'fs':
        return <FolderOpenOutlined />
      case 's3':
        return <ApiOutlined />
      default:
        return <RocketOutlined />
    }
  }

  const getTypeName = (type: string) => {
    switch (type) {
      case 'kv':
        return 'KV 存储'
      case 'metadata':
        return '元数据'
      case 'fs':
        return '文件系统'
      case 's3':
        return 'S3 存储'
      default:
        return type
    }
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'success'
      case 'running':
        return 'processing'
      case 'failed':
        return 'error'
      default:
        return 'default'
    }
  }

  const recentResults = results.slice(0, 5)

  const summaryColumns = [
    {
      title: '类型',
      dataIndex: 'type',
      key: 'type',
      render: (type: string) => (
        <Space>
          {getTypeIcon(type)}
          <span>{getTypeName(type)}</span>
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={getStatusColor(status)}>
          {status === 'completed' ? '已完成' : status === 'running' ? '运行中' : '失败'}
        </Tag>
      ),
    },
    {
      title: '开始时间',
      dataIndex: 'started_at',
      key: 'started_at',
      render: (time: string) => new Date(time).toLocaleString(),
    },
    {
      title: '耗时',
      key: 'duration',
      render: (_: unknown, record: BenchmarkResult) => {
        if (!record.completed_at) return '-'
        const start = new Date(record.started_at).getTime()
        const end = new Date(record.completed_at).getTime()
        const ms = end - start
        if (ms < 1000) return `${ms}ms`
        if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
        return `${(ms / 60000).toFixed(1)}min`
      },
    },
    {
      title: '操作',
      key: 'action',
      render: (_: unknown, record: BenchmarkResult) => (
        <Button
          type="link"
          icon={<EyeOutlined />}
          onClick={() => handleViewDetail(record)}
          disabled={record.status !== 'completed'}
        >
          查看详情
        </Button>
      ),
    },
  ]

  const getChartOption = (report: BenchmarkReport): EChartsOption => {
    const operations = Object.keys(report.summary)
    const values = operations.map(op => {
      const data = report.summary[op]
      return data.avg_ops_per_sec || data.avg_bandwidth_mbps || 0
    })

    return {
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
      },
      xAxis: {
        type: 'category',
        data: operations,
        axisLabel: { rotate: 30, fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: (value: number) => {
            if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`
            if (value >= 1000) return `${(value / 1000).toFixed(1)}K`
            return value.toFixed(0)
          },
        },
      },
      series: [
        {
          type: 'bar',
          data: values,
          barWidth: '60%',
          itemStyle: {
            borderRadius: [4, 4, 0, 0],
            color: {
              type: 'linear',
              x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: '#00d9ff' },
                { offset: 1, color: '#00ff88' },
              ],
            },
          },
        },
      ],
    }
  }

  const getSummaryStats = (report: BenchmarkReport) => {
    const summary = report.summary
    const ops = Object.entries(summary).filter(([_, v]) => v.avg_ops_per_sec)
    const bw = Object.entries(summary).filter(([_, v]) => v.avg_bandwidth_mbps)

    const avgOps = ops.length > 0
      ? ops.reduce((sum, [_, v]) => sum + (v.avg_ops_per_sec || 0), 0) / ops.length
      : 0
    const avgLatency = Object.values(summary).reduce((sum, v) => sum + (v.avg_latency_ms || 0), 0) / Object.keys(summary).length
    const avgBw = bw.length > 0
      ? bw.reduce((sum, [_, v]) => sum + (v.avg_bandwidth_mbps || 0), 0) / bw.length
      : 0

    return { avgOps, avgLatency, avgBw }
  }

  const stats = currentReport ? getSummaryStats(currentReport) : null

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <Title level={4} style={{ margin: 0 }}>性能测试</Title>
          <Text type="secondary">PowerFS 基准测试与性能展示</Text>
        </div>
        <RefreshControl onRefresh={loadData} loading={loading} />
      </div>

      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col xs={24} sm={6}>
          <Button
            type={selectedTab === 'kv' ? 'primary' : 'default'}
            block
            icon={<DatabaseOutlined />}
            onClick={() => setSelectedTab('kv')}
          >
            KV 存储测试
          </Button>
        </Col>
        <Col xs={24} sm={6}>
          <Button
            type={selectedTab === 'metadata' ? 'primary' : 'default'}
            block
            icon={<FileTextOutlined />}
            onClick={() => setSelectedTab('metadata')}
          >
            元数据测试
          </Button>
        </Col>
        <Col xs={24} sm={6}>
          <Button
            type={selectedTab === 'fs' ? 'primary' : 'default'}
            block
            icon={<FolderOpenOutlined />}
            onClick={() => setSelectedTab('fs')}
          >
            文件系统测试
          </Button>
        </Col>
        <Col xs={24} sm={6}>
          <Button
            type={selectedTab === 's3' ? 'primary' : 'default'}
            block
            icon={<ApiOutlined />}
            onClick={() => setSelectedTab('s3')}
          >
            S3 存储测试
          </Button>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col xs={24} lg={8}>
          <Card
            title={<Space><ApiOutlined />测试配置</Space>}
            style={{ borderRadius: 12 }}
            styles={{ body: { padding: 20 } }}
          >
            {currentReport ? (
              <Space direction="vertical" style={{ width: '100%', gap: 12 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <Text type="secondary">测试轮数</Text>
                  <Text strong>{currentReport.config.rounds} 轮</Text>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <Text type="secondary">每轮迭代</Text>
                  <Text strong>{currentReport.config.iterations_per_round.toLocaleString()} 次</Text>
                </div>
                {currentReport.config.data_size_bytes && (
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Text type="secondary">数据大小</Text>
                    <Text strong>{(currentReport.config.data_size_bytes / 1024).toFixed(0)} KB</Text>
                  </div>
                )}
                {currentReport.config.test_sizes && (
                  <div>
                    <Text type="secondary">测试文件大小</Text>
                    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 8 }}>
                      {currentReport.config.test_sizes.map((size, i) => (
                        <Tag key={i} color="blue">{(size / 1024).toFixed(0)}KB</Tag>
                      ))}
                    </div>
                  </div>
                )}
                <Divider style={{ margin: '12px 0' }} />
                <Button
                  type="primary"
                  icon={<PlayCircleOutlined />}
                  loading={runningType === selectedTab}
                  block
                  onClick={() => handleRunBenchmark(selectedTab)}
                >
                  {runningType === selectedTab ? '测试进行中...' : '运行测试'}
                </Button>
              </Space>
            ) : (
              <div style={{ textAlign: 'center', padding: '40px 0' }}>
                <RocketOutlined style={{ fontSize: 48, color: 'var(--pf-color-primary)', marginBottom: 12 }} />
                <div>暂无测试数据</div>
                <Button
                  type="primary"
                  icon={<PlayCircleOutlined />}
                  loading={runningType === selectedTab}
                  style={{ marginTop: 16 }}
                  onClick={() => handleRunBenchmark(selectedTab)}
                >
                  运行{getTypeName(selectedTab)}测试
                </Button>
              </div>
            )}
          </Card>
        </Col>

        <Col xs={24} lg={16}>
          <Card
            title={<Space><RocketOutlined />性能指标</Space>}
            style={{ borderRadius: 12 }}
            styles={{ body: { padding: 20 } }}
          >
            {currentReport ? (
              <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
                <Row gutter={[16, 16]}>
                  <Col xs={24} sm={8}>
                    <StatCard
                      title={selectedTab === 'fs' ? '平均带宽' : '平均吞吐量'}
                      value={selectedTab === 'fs' && stats?.avgBw ? stats.avgBw : stats?.avgOps || 0}
                      suffix={selectedTab === 'fs' ? ' MB/s' : ' ops/s'}
                      precision={2}
                      icon={<ApiOutlined />}
                      status="active"
                    />
                  </Col>
                  <Col xs={24} sm={8}>
                    <StatCard
                      title="平均延迟"
                      value={stats?.avgLatency || 0}
                      suffix=" ms"
                      precision={4}
                      icon={<ClockCircleOutlined />}
                      status="active"
                    />
                  </Col>
                  <Col xs={24} sm={8}>
                    <StatCard
                      title="操作数"
                      value={Object.keys(currentReport.summary).length}
                      suffix=" 项"
                      icon={<FileTextOutlined />}
                      status="active"
                    />
                  </Col>
                </Row>

                <MetricChart option={getChartOption(currentReport)} height={300} />
              </Space>
            ) : (
              <div style={{ textAlign: 'center', padding: '60px 0', color: 'var(--pf-color-text-secondary)' }}>
                <DatabaseOutlined style={{ fontSize: 48, marginBottom: 12, opacity: 0.3 }} />
                <div>点击上方按钮运行测试以查看性能数据</div>
              </div>
            )}
          </Card>
        </Col>
      </Row>

      <Card
        title={<Space><RestOutlined />测试结果详情</Space>}
        style={{ borderRadius: 12 }}
        styles={{ body: { padding: 20 } }}
      >
        {currentReport ? (
          <Table
            dataSource={Object.entries(currentReport.summary).map(([op, data]) => ({
              key: op,
              operation: op,
              ops: data.avg_ops_per_sec,
              bw: data.avg_bandwidth_mbps,
              latency: data.avg_latency_ms ?? 0,
            }))}
            columns={[
              { title: '操作', dataIndex: 'operation', key: 'operation' },
              {
                title: selectedTab === 'fs' ? '带宽 (MB/s)' : '吞吐量 (ops/s)',
                key: 'value',
                render: (_: unknown, record: { ops?: number; bw?: number }) => (
                  <Text strong style={{ color: '#00ff88', fontSize: 16 }}>
                    {record.bw ? record.bw.toFixed(2) : record.ops?.toLocaleString()}
                  </Text>
                ),
              },
              {
                title: '延迟 (ms)',
                dataIndex: 'latency',
                key: 'latency',
                render: (latency: number) => (
                  <Text style={{ color: '#00d9ff' }}>{latency.toFixed(4)}</Text>
                ),
              },
              {
                title: '性能等级',
                key: 'level',
                render: (_: unknown, record: { ops?: number; bw?: number; latency: number }) => {
                  const value = record.bw || record.ops || 0
                  let level = '一般'
                  let color = 'default'
                  if ((selectedTab === 'fs' && value > 20000) || (selectedTab !== 'fs' && value > 1000000)) {
                    level = '优秀'
                    color = 'success'
                  } else if ((selectedTab === 'fs' && value > 5000) || (selectedTab !== 'fs' && value > 10000)) {
                    level = '良好'
                    color = 'blue'
                  } else if ((selectedTab === 'fs' && value > 1000) || (selectedTab !== 'fs' && value > 1000)) {
                    level = '一般'
                    color = 'warning'
                  }
                  return <Tag color={color}>{level}</Tag>
                },
              },
            ]}
            pagination={false}
          />
        ) : (
          <div style={{ textAlign: 'center', padding: '40px 0', color: 'var(--pf-color-text-secondary)' }}>
            暂无测试数据
          </div>
        )}
      </Card>

      <Card
        title={<Space><ClockCircleOutlined />最近测试记录</Space>}
        style={{ borderRadius: 12, marginTop: 24 }}
        styles={{ body: { padding: 20 } }}
      >
        <Table
          dataSource={recentResults}
          columns={summaryColumns}
          rowKey="id"
          pagination={false}
          size="small"
        />
      </Card>

      <Modal
        title={<Space>{detailData && getTypeIcon(detailData.type)} 测试详情</Space>}
        open={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        width={800}
        footer={null}
        loading={detailLoading}
      >
        {detailData && detailData.result && (
          <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
            <Descriptions bordered column={2} size="small">
              <Descriptions.Item label="测试类型">{getTypeName(detailData.type)}</Descriptions.Item>
              <Descriptions.Item label="测试状态">
                <Tag color={getStatusColor(detailData.status)}>
                  {detailData.status === 'completed' ? '已完成' : detailData.status === 'running' ? '运行中' : '失败'}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="开始时间">{new Date(detailData.started_at).toLocaleString()}</Descriptions.Item>
              <Descriptions.Item label="完成时间">
                {detailData.completed_at ? new Date(detailData.completed_at).toLocaleString() : '-'}
              </Descriptions.Item>
              <Descriptions.Item label="耗时" span={2}>
                {detailData.completed_at ? (() => {
                  const start = new Date(detailData.started_at).getTime()
                  const end = new Date(detailData.completed_at).getTime()
                  const ms = end - start
                  if (ms < 1000) return `${ms}ms`
                  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
                  return `${(ms / 60000).toFixed(1)}min`
                })() : '-'}
              </Descriptions.Item>
            </Descriptions>

            <Divider>测试配置</Divider>
            <Descriptions bordered column={2} size="small">
              <Descriptions.Item label="测试轮数">
                {detailData.result.config.rounds} 轮
              </Descriptions.Item>
              <Descriptions.Item label="每轮迭代">
                {detailData.result.config.iterations_per_round} 次
              </Descriptions.Item>
              {detailData.result.config.data_size_bytes && (
                <Descriptions.Item label="数据大小">
                  {(detailData.result.config.data_size_bytes / 1024).toFixed(0)} KB
                </Descriptions.Item>
              )}
              {detailData.result.config.test_sizes && (
                <Descriptions.Item label="测试文件大小" span={2}>
                  {detailData.result.config.test_sizes.map((size: number, i: number) => (
                    <Tag key={i} color="blue" style={{ marginRight: 8 }}>
                      {(size / 1024).toFixed(0)}KB
                    </Tag>
                  ))}
                </Descriptions.Item>
              )}
            </Descriptions>

            <Divider>操作详情</Divider>
            <Table
              dataSource={detailData.result.operations.map((op, i) => ({
                key: i,
                operation: op.operation,
                count: op.count,
                duration: op.duration_ms,
                opsPerSec: op.ops_per_sec,
                latency: op.avg_latency_ms,
                bandwidth: op.bandwidth_mbps,
              }))}
              columns={[
                { title: '操作', dataIndex: 'operation', key: 'operation' },
                { title: '次数', dataIndex: 'count', key: 'count', render: (v: number) => v.toLocaleString() },
                { title: '耗时 (ms)', dataIndex: 'duration', key: 'duration', render: (v: number) => v.toFixed(4) },
                { title: '吞吐量 (ops/s)', dataIndex: 'opsPerSec', key: 'opsPerSec', render: (v: number) => v.toLocaleString() },
                { title: '延迟 (ms)', dataIndex: 'latency', key: 'latency', render: (v: number) => v.toFixed(4) },
                { title: '带宽 (MB/s)', dataIndex: 'bandwidth', key: 'bandwidth', render: (v: number | undefined) => v ? v.toFixed(2) : '-' },
              ]}
              pagination={false}
              size="small"
            />

            <Divider>统计摘要</Divider>
            <Table
              dataSource={Object.entries(detailData.result.summary).map(([op, data]) => ({
                key: op,
                operation: op,
                ops: data.avg_ops_per_sec,
                bw: data.avg_bandwidth_mbps,
                latency: data.avg_latency_ms ?? 0,
              }))}
              columns={[
                { title: '操作', dataIndex: 'operation', key: 'operation' },
                {
                  title: detailData.type === 'fs' ? '带宽 (MB/s)' : '吞吐量 (ops/s)',
                  key: 'value',
                  render: (_: unknown, record: { ops?: number; bw?: number }) => (
                    <Text strong style={{ color: '#00ff88', fontSize: 14 }}>
                      {record.bw ? record.bw.toFixed(2) : record.ops?.toLocaleString()}
                    </Text>
                  ),
                },
                {
                  title: '平均延迟 (ms)',
                  dataIndex: 'latency',
                  key: 'latency',
                  render: (latency: number) => (
                    <Text style={{ color: '#00d9ff' }}>{latency.toFixed(4)}</Text>
                  ),
                },
              ]}
              pagination={false}
              size="small"
            />
          </Space>
        )}
      </Modal>
    </div>
  )
}

export default Benchmark