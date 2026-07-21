import { useEffect, useState } from 'react'
import { Card, Table, Tag, Button, Modal, Space, Progress, message, Form, Input, Tabs, Descriptions, Typography } from 'antd'
const { Text } = Typography
import {
  KeyOutlined,
  DeleteOutlined,
  EyeOutlined,
  WarningOutlined,
  ApiOutlined,
  PlusOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import type { KVSessionInfo, KVMetrics, KVNamespace } from '@/types'
import {
  getKVSessions,
  getKVMetrics,
  deleteKVSession,
  createKVNamespace,
  listKVNamespaces,
  deleteKVNamespace,
} from '@/services/api'
import { formatBytes, formatPercent, formatNumber } from '@/utils/format'
import { generateTimeSeriesData } from '@/utils/mockData'

function KV() {
  const [sessions, setSessions] = useState<KVSessionInfo[]>([])
  const [metrics, setMetrics] = useState<KVMetrics | null>(null)
  const [selectedSession, setSelectedSession] = useState<KVSessionInfo | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [hitRatioTrend] = useState(generateTimeSeriesData(24, 90, 10))

  const [namespaces, setNamespaces] = useState<KVNamespace[]>([])
  const [showCreateNamespace, setShowCreateNamespace] = useState(false)
  const [namespaceForm] = Form.useForm()

  useEffect(() => {
    loadKVData()
    const interval = setInterval(loadKVData, 10000)
    return () => clearInterval(interval)
  }, [])

  const loadKVData = async () => {
    const [sessionList, kvMetrics] = await Promise.all([
      getKVSessions(),
      getKVMetrics(),
    ])
    setSessions(sessionList)
    setMetrics(kvMetrics)
  }

  useEffect(() => {
    loadNamespaces()
  }, [])

  const loadNamespaces = async () => {
    const ns = await listKVNamespaces()
    setNamespaces(ns)
  }

  const handleViewDetail = (session: KVSessionInfo) => {
    setSelectedSession(session)
    setShowDetail(true)
  }

  const handleDeleteSession = (session: KVSessionInfo) => {
    setSelectedSession(session)
    setShowDeleteConfirm(true)
  }

  const confirmDeleteSession = async () => {
    if (selectedSession) {
      await deleteKVSession(selectedSession.id)
      message.success('会话删除成功')
      setShowDeleteConfirm(false)
      loadKVData()
    }
  }

  const handleCreateNamespace = async () => {
    try {
      const values = await namespaceForm.validateFields()
      await createKVNamespace(values.name)
      message.success('命名空间创建成功')
      setShowCreateNamespace(false)
      namespaceForm.resetFields()
      loadNamespaces()
    } catch (error) {
      message.error('创建失败')
    }
  }

  const handleDeleteNamespace = async (id: string) => {
    try {
      await deleteKVNamespace(id)
      message.success('命名空间删除成功')
      loadNamespaces()
    } catch (error) {
      message.error('删除失败')
    }
  }

  const sessionColumns = [
    {
      title: '会话ID',
      dataIndex: 'id',
      key: 'id',
      width: 150,
    },
    {
      title: '模型名称',
      dataIndex: 'model_name',
      key: 'model_name',
      width: 150,
      render: (name: string) => (
        <Tag color="purple">{name}</Tag>
      ),
    },
    {
      title: '层数',
      dataIndex: 'layer_count',
      key: 'layer_count',
      width: 80,
    },
    {
      title: 'Block数',
      dataIndex: 'block_count',
      key: 'block_count',
      width: 100,
      render: (count: number) => count.toLocaleString(),
    },
    {
      title: '内存使用',
      key: 'memory',
      width: 150,
      render: (_: unknown, record: KVSessionInfo) => formatBytes(record.memory_used),
    },
    {
      title: '命中率',
      key: 'hit_ratio',
      width: 120,
      render: (_: unknown, record: KVSessionInfo) => (
        <div>
          <Progress
            percent={record.hit_ratio}
            size="small"
            strokeColor={record.hit_ratio >= 90 ? '#52c41a' : record.hit_ratio >= 80 ? '#faad14' : '#f5222d'}
            showInfo={false}
          />
          <span style={{ marginLeft: 8, fontSize: 12, color: record.hit_ratio >= 90 ? '#52c41a' : '#fa8c16' }}>
            {formatPercent(record.hit_ratio)}
          </span>
        </div>
      ),
    },
    {
      title: '驱逐次数',
      dataIndex: 'eviction_count',
      key: 'eviction_count',
      width: 100,
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (time: string) => new Date(time).toLocaleString(),
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      render: (_: unknown, record: KVSessionInfo) => (
        <Space>
          <Button
            type="text"
            icon={<EyeOutlined />}
            onClick={() => handleViewDetail(record)}
          >
            详情
          </Button>
          <Button
            type="text"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDeleteSession(record)}
          >
            删除
          </Button>
        </Space>
      ),
    },
  ]

  const namespaceColumns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 200,
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      width: 200,
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (time: number) => new Date(time * 1000).toLocaleString(),
    },
    {
      title: '更新时间',
      dataIndex: 'updated_at',
      key: 'updated_at',
      width: 180,
      render: (time: number) => new Date(time * 1000).toLocaleString(),
    },
    {
      title: '操作',
      key: 'action',
      width: 100,
      render: (_: unknown, record: KVNamespace) => (
        <Button
          type="text"
          danger
          icon={<DeleteOutlined />}
          onClick={() => handleDeleteNamespace(record.id)}
        >
          删除
        </Button>
      ),
    },
  ]

  const SessionMonitor = () => (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            KV 存储是 PowerFS 的分布式键值存储组件，用于存储文件系统的元数据和索引数据。
            会话是客户端与 KV 存储之间的连接，命名空间用于隔离不同应用的数据。
          </Text>
        </div>
      </Card>

      <div style={{ marginBottom: 16, display: 'flex', gap: 16 }}>
        <div style={{ flex: 1 }}>
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
                <span style={{ color: '#8c8c8c' }}>会话数量</span>
              </div>
              <span style={{ fontSize: 32, fontWeight: 'bold', color: '#fa8c16' }}>
                {metrics?.session_count || 0}
              </span>
            </Space>
          </Card>
        </div>
        <div style={{ flex: 1 }}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#f6ffed', padding: 8, borderRadius: 8 }}>
                  <ApiOutlined style={{ fontSize: 24, color: '#52c41a' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>总Block数</span>
              </div>
              <span style={{ fontSize: 32, fontWeight: 'bold', color: '#52c41a' }}>
                {formatNumber(metrics?.block_count || 0)}
              </span>
            </Space>
          </Card>
        </div>
        <div style={{ flex: 1 }}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#e6f7ff', padding: 8, borderRadius: 8 }}>
                  <WarningOutlined style={{ fontSize: 24, color: '#1890ff' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>命中率</span>
              </div>
              <span style={{ fontSize: 32, fontWeight: 'bold', color: metrics?.hit_ratio && metrics.hit_ratio >= 90 ? '#52c41a' : '#faad14' }}>
                {formatPercent(metrics?.hit_ratio || 0)}
              </span>
            </Space>
          </Card>
        </div>
        <div style={{ flex: 1 }}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#fff0f6', padding: 8, borderRadius: 8 }}>
                  <KeyOutlined style={{ fontSize: 24, color: '#eb2f96' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>内存使用</span>
              </div>
              <span style={{ fontSize: 32, fontWeight: 'bold', color: '#eb2f96' }}>
                {formatBytes(metrics?.memory_used || 0)}
              </span>
            </Space>
          </Card>
        </div>
      </div>

      <div style={{ marginBottom: 16, display: 'flex', gap: 16 }}>
        <div style={{ flex: 1 }}>
          <Card
            title="命中率趋势"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <ReactECharts
              option={{
                tooltip: {
                  trigger: 'axis',
                  formatter: '{b}<br/>命中率: {c}%',
                },
                grid: {
                  left: '3%',
                  right: '4%',
                  bottom: '3%',
                  containLabel: true,
                },
                xAxis: {
                  type: 'category',
                  data: hitRatioTrend.map(d => {
                    const date = new Date(d.time)
                    return `${date.getHours()}:00`
                  }),
                  axisLine: { lineStyle: { color: '#d9d9d9' } },
                  axisLabel: { color: '#8c8c8c' },
                },
                yAxis: {
                  type: 'value',
                  min: 70,
                  max: 100,
                  axisLine: { show: false },
                  axisTick: { show: false },
                  splitLine: { lineStyle: { color: '#f0f0f0' } },
                  axisLabel: { color: '#8c8c8c', formatter: '{value}%' },
                },
                series: [
                  {
                    name: '命中率',
                    type: 'line',
                    smooth: true,
                    data: hitRatioTrend.map(d => d.value),
                    areaStyle: {
                      color: {
                        type: 'linear',
                        x: 0,
                        y: 0,
                        x2: 0,
                        y2: 1,
                        colorStops: [
                          { offset: 0, color: 'rgba(82, 196, 26, 0.3)' },
                          { offset: 1, color: 'rgba(82, 196, 26, 0.05)' },
                        ],
                      },
                    },
                    lineStyle: { color: '#52c41a', width: 3 },
                    itemStyle: { color: '#52c41a' },
                  },
                ],
              }}
              style={{ height: 300 }}
            />
          </Card>
        </div>
        <div style={{ flex: 1 }}>
          <Card
            title="缓存统计"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>总请求数</span>
                <span style={{ fontWeight: 500 }}>{formatNumber((metrics?.put_count || 0) + (metrics?.get_count || 0))} 次</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>Put请求</span>
                <span style={{ fontWeight: 500 }}>{formatNumber(metrics?.put_count || 0)} 次</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>Get请求</span>
                <span style={{ fontWeight: 500 }}>{formatNumber(metrics?.get_count || 0)} 次</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>驱逐次数</span>
                <span style={{ fontWeight: 500 }}>{metrics?.eviction_count || 0} 次</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>平均延迟</span>
                <span style={{ fontWeight: 500 }}>{(metrics?.avg_latency || 0).toFixed(2)} ms</span>
              </div>
            </Space>
          </Card>
        </div>
      </div>

      <Card
        title="KV会话列表"
        style={{ borderRadius: 12 }}
      >
        <Table
          columns={sessionColumns}
          dataSource={sessions}
          rowKey="id"
          pagination={{ pageSize: 10 }}
          scroll={{ x: 1200 }}
        />
      </Card>

      <Modal
        title="会话详情"
        open={showDetail}
        onCancel={() => setShowDetail(false)}
        footer={null}
        width={500}
      >
        {selectedSession && (
          <Space direction="vertical" style={{ width: '100%', gap: 20 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ background: '#fff7e6', padding: 12, borderRadius: 12 }}>
                <KeyOutlined style={{ fontSize: 32, color: '#fa8c16' }} />
              </div>
              <div>
                <h3 style={{ margin: 0 }}>{selectedSession.id}</h3>
                <Tag color="purple">{selectedSession.model_name}</Tag>
              </div>
            </div>

            <div>
              <h4 style={{ margin: '0 0 12px' }}>缓存统计</h4>
              <Space direction="vertical" style={{ width: '100%', gap: 12 }}>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ color: '#8c8c8c' }}>内存使用</span>
                    <span>{formatBytes(selectedSession.memory_used)}</span>
                  </div>
                  <Progress percent={Math.min((selectedSession.memory_used / 21474836480) * 100, 100)} />
                </div>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ color: '#8c8c8c' }}>命中率</span>
                    <span>{formatPercent(selectedSession.hit_ratio)}</span>
                  </div>
                  <Progress
                    percent={selectedSession.hit_ratio}
                    strokeColor={selectedSession.hit_ratio >= 90 ? '#52c41a' : '#faad14'}
                  />
                </div>
              </Space>
            </div>

            <div>
              <h4 style={{ margin: '0 0 12px' }}>基本信息</h4>
              <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>层数</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedSession.layer_count} 层</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>Block数</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedSession.block_count} 个</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>驱逐次数</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedSession.eviction_count} 次</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>创建时间</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{new Date(selectedSession.created_at).toLocaleString()}</p>
                </div>
              </div>
            </div>
          </Space>
        )}
      </Modal>

      <Modal
        title="确认删除"
        open={showDeleteConfirm}
        onCancel={() => setShowDeleteConfirm(false)}
        onOk={confirmDeleteSession}
        okText="确认删除"
        cancelText="取消"
        okButtonProps={{ danger: true }}
      >
        <p>确定要删除会话 <strong>{selectedSession?.id}</strong> 吗？</p>
        <p style={{ color: '#8c8c8c', fontSize: 12 }}>删除后该会话的所有Block数据将被清理。</p>
      </Modal>
    </div>
  )

  const NamespaceManagement = () => (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setShowCreateNamespace(true)}>
          创建命名空间
        </Button>
      </div>

      <Card
        title="命名空间列表"
        style={{ borderRadius: 12 }}
      >
        <Table
          columns={namespaceColumns}
          dataSource={namespaces}
          rowKey="id"
          pagination={{ pageSize: 10 }}
          scroll={{ x: 800 }}
        />
      </Card>
    </div>
  )

  return (
    <div>
      <Tabs
        defaultActiveKey="sessions"
        items={[
          {
            key: 'sessions',
            label: '会话监控',
            children: <SessionMonitor />,
          },
          {
            key: 'namespaces',
            label: '命名空间管理',
            children: <NamespaceManagement />,
          },
        ]}
      />

      <Card title="常见问题" size="small" style={{ marginTop: 24 }}>
        <Descriptions column={1} size="small">
          <Descriptions.Item label="什么是 KV 存储？">
            KV（键值）存储是 PowerFS 的分布式存储组件，用于存储文件系统的元数据、索引和配置信息。它提供高性能的读写能力，支持自动分片和数据冗余。
          </Descriptions.Item>
          <Descriptions.Item label="什么是会话（Session）？">
            会话是客户端与 KV 存储之间的逻辑连接。每个会话维护自己的缓存空间，包含多个数据层（Layer）和数据块（Block）。
          </Descriptions.Item>
          <Descriptions.Item label="什么是命名空间（Namespace）？">
            命名空间用于隔离不同应用或用户的数据。不同命名空间之间的数据是完全隔离的，可用于多租户场景。
          </Descriptions.Item>
          <Descriptions.Item label="命中率是什么？">
            命中率是指缓存命中的比例。较高的命中率（通常 &gt;90%）表示缓存工作正常，大多数请求可以直接从缓存返回，不需要访问后端存储。
          </Descriptions.Item>
          <Descriptions.Item label="驱逐次数是什么？">
            当缓存空间不足时，系统会按照 LRU 策略淘汰最久未使用的数据块，这个过程称为驱逐。频繁的驱逐可能意味着缓存空间不足。
          </Descriptions.Item>
        </Descriptions>
      </Card>

      <Modal
        title="创建命名空间"
        open={showCreateNamespace}
        onCancel={() => { setShowCreateNamespace(false); namespaceForm.resetFields(); }}
        onOk={handleCreateNamespace}
      >
        <Form form={namespaceForm} layout="vertical">
          <Form.Item
            name="name"
            label="名称"
            rules={[{ required: true, message: '请输入命名空间名称' }]}
          >
            <Input placeholder="请输入命名空间名称" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

export default KV
