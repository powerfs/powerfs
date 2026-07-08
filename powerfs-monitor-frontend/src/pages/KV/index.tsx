import { useEffect, useState } from 'react'
import { Card, Table, Tag, Button, Modal, Space, Progress, message, Form, Input, Tabs } from 'antd'
import {
  KeyOutlined,
  DeleteOutlined,
  EyeOutlined,
  WarningOutlined,
  ApiOutlined,
  PlusOutlined,
  CopyOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
} from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import type { KVSessionInfo, KVMetrics, KVNamespace, KVAccessKey } from '@/types'
import {
  getKVSessions,
  getKVMetrics,
  deleteKVSession,
  createKVNamespace,
  listKVNamespaces,
  deleteKVNamespace,
  createKVKey,
  listKVKeys,
  deleteKVKey,
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

  const [keys, setKeys] = useState<KVAccessKey[]>([])
  const [showCreateKey, setShowCreateKey] = useState(false)
  const [newKey, setNewKey] = useState<{ access_key: string; secret_key: string } | null>(null)

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

  useEffect(() => {
    loadKeys()
  }, [])

  const loadKeys = async () => {
    const k = await listKVKeys()
    setKeys(k)
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

  const handleCreateKey = async () => {
    try {
      const key = await createKVKey()
      setNewKey({ access_key: key.access_key, secret_key: key.secret_key })
      setShowCreateKey(true)
      loadKeys()
    } catch (error) {
      message.error('创建失败')
    }
  }

  const handleDeleteKey = async (id: string) => {
    try {
      await deleteKVKey(id)
      message.success('API Key 删除成功')
      loadKeys()
    } catch (error) {
      message.error('删除失败')
    }
  }

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text)
      message.success('已复制到剪贴板')
    } catch (error) {
      message.error('复制失败')
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

  const keyColumns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 150,
    },
    {
      title: 'Access Key',
      dataIndex: 'access_key',
      key: 'access_key',
      width: 250,
      render: (key: string) => (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontFamily: 'monospace', fontSize: 12 }}>{key}</span>
          <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => copyToClipboard(key)} />
        </div>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => (
        <Tag color={status === 'active' ? 'green' : 'red'}>
          {status === 'active' ? '活跃' : '停用'}
        </Tag>
      ),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (time: string) => new Date(time).toLocaleString(),
    },
    {
      title: '最后使用',
      dataIndex: 'last_used_at',
      key: 'last_used_at',
      width: 180,
      render: (time: string | undefined) => time ? new Date(time).toLocaleString() : '-',
    },
    {
      title: '操作',
      key: 'action',
      width: 100,
      render: (_: unknown, record: KVAccessKey) => (
        <Button
          type="text"
          danger
          icon={<DeleteOutlined />}
          onClick={() => handleDeleteKey(record.id)}
        >
          删除
        </Button>
      ),
    },
  ]

  const SessionMonitor = () => (
    <div>
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

  const APIKeyManagement = () => (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreateKey}>
          创建 API Key
        </Button>
      </div>

      <Card
        title="API Key 列表"
        style={{ borderRadius: 12 }}
      >
        <Table
          columns={keyColumns}
          dataSource={keys}
          rowKey="id"
          pagination={{ pageSize: 10 }}
          scroll={{ x: 1000 }}
        />
      </Card>

      <Modal
        title="API Key 创建成功"
        open={showCreateKey}
        onCancel={() => { setShowCreateKey(false); setNewKey(null); }}
        footer={null}
        width={500}
      >
        {newKey && (
          <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ background: '#f6ffed', padding: 12, borderRadius: 12 }}>
                <CheckCircleOutlined style={{ fontSize: 32, color: '#52c41a' }} />
              </div>
              <div>
                <h3 style={{ margin: 0 }}>API Key 创建成功</h3>
                <p style={{ color: '#8c8c8c', fontSize: 12, margin: '4px 0' }}>请妥善保存您的 Secret Key，它只会显示一次</p>
              </div>
            </div>

            <div>
              <h4 style={{ margin: '0 0 8px' }}>Access Key</h4>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Input
                  value={newKey.access_key}
                  readOnly
                  style={{ fontFamily: 'monospace' }}
                />
                <Button icon={<CopyOutlined />} onClick={() => copyToClipboard(newKey.access_key)} />
              </div>
            </div>

            <div>
              <h4 style={{ margin: '0 0 8px' }}>Secret Key</h4>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Input
                  value={newKey.secret_key}
                  readOnly
                  style={{ fontFamily: 'monospace' }}
                />
                <Button icon={<CopyOutlined />} onClick={() => copyToClipboard(newKey.secret_key)} />
              </div>
            </div>

            <div style={{ background: '#fff7e6', padding: 12, borderRadius: 8, display: 'flex', alignItems: 'flex-start', gap: 8 }}>
              <ExclamationCircleOutlined style={{ color: '#fa8c16', fontSize: 16, marginTop: 2 }} />
              <div>
                <p style={{ margin: 0, fontSize: 12, color: '#fa8c16' }}>重要提示</p>
                <p style={{ margin: '4px 0 0', fontSize: 12, color: '#8c8c8c' }}>
                  Secret Key 只会显示一次，丢失后无法找回。请立即复制保存到安全的地方。
                </p>
              </div>
            </div>
          </Space>
        )}
      </Modal>
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
          {
            key: 'keys',
            label: 'API Key 管理',
            children: <APIKeyManagement />,
          },
        ]}
      />

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
