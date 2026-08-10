import { useEffect, useState, useMemo, useCallback } from 'react'
import {
  Card,
  Table,
  Button,
  Space,
  Progress,
  message,
  Tabs,
  Drawer,
  Input,
  Segmented,
  Tooltip,
  Typography,
  Modal,
  Descriptions,
  Tag,
} from 'antd'
import {
  DeleteOutlined,
  EyeOutlined,
  PlusOutlined,
  HddOutlined,
  CloudServerOutlined,
  DatabaseOutlined,
  ReloadOutlined,
  SearchOutlined,
  ThunderboltOutlined,
  ApiOutlined,
  InfoCircleOutlined,
  DashboardOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { SizeType } from 'antd/es/config-provider/SizeContext'
import type { NodeInfo, StorageDevice } from '@/types'
import { getNodes, getDevices, deleteNode } from '@/services/api'
import { formatBytes, formatPercent, formatUptime, formatTime } from '@/utils/format'
import {
  KpiBar,
  StatusTag,
  EmptyState,
  SkeletonCard,
  type StatCardProps,
} from '@/components/pro'
import { resolveNodeStatus, raftRolePalette } from '@/styles/status'
import { useMetricStream } from '@/hooks/useMetricStream'

const { Text, Title } = Typography

type NodeRoleTab = 'all' | 'master' | 'filer' | 'volume'

function Nodes() {
  const { t } = useTranslation(['common', 'nav'])
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [selectedNode, setSelectedNode] = useState<NodeInfo | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [nodeDevices, setNodeDevices] = useState<StorageDevice[]>([])
  const [loading, setLoading] = useState(true)
  const [density, setDensity] = useState<SizeType>('middle')
  const [search, setSearch] = useState('')
  const [activeTab, setActiveTab] = useState<NodeRoleTab>('all')

  const loadNodes = useCallback(async () => {
    try {
      const data = await getNodes()
      setNodes(data)
    } catch (e) {
      console.error('Failed to load nodes:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadNodes()
    // Backoff polling to 30s — real-time updates flow through WS.
    const interval = setInterval(() => void loadNodes(), 30000)
    return () => clearInterval(interval)
  }, [loadNodes])

  // Live merge: WS pushes either a single NodeInfo (event-driven) or the
  // full list snapshot (on connect). Both update by node_id.
  useMetricStream({
    source: 'nodes',
    onMetricUpdate: (u) => {
      const payload = u.payload
      if (Array.isArray(payload)) {
        setNodes(payload as NodeInfo[])
      } else if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
        const node = payload as NodeInfo
        setNodes((prev) => {
          const idx = prev.findIndex((n) => n.id === node.id)
          if (idx === -1) return [...prev, node]
          const next = [...prev]
          next[idx] = { ...next[idx], ...node }
          return next
        })
      }
    },
  })

  const loadNodeDevices = useCallback(async (nodeId: string) => {
    try {
      const data = await getDevices(nodeId)
      setNodeDevices(data)
    } catch (e) {
      console.error('Failed to load devices:', e)
      setNodeDevices([])
    }
  }, [])

  const handleViewDetail = (node: NodeInfo) => {
    setSelectedNode(node)
    setShowDetail(true)
    void loadNodeDevices(node.id)
  }

  const handleDelete = (node: NodeInfo) => {
    setSelectedNode(node)
    setShowDeleteConfirm(true)
  }

  const confirmDelete = async () => {
    if (!selectedNode) return
    try {
      await deleteNode(selectedNode.id)
      message.success(`Node ${selectedNode.id} removed`)
      setNodes(prev => prev.filter(n => n.id !== selectedNode.id))
    } catch (e) {
      message.error('Failed to delete node')
      console.error(e)
    } finally {
      setShowDeleteConfirm(false)
    }
  }

  // ── KPI summary ──
  const kpiItems: StatCardProps[] = useMemo(() => {
    const total = nodes.length
    const online = nodes.filter(n => n.status === 'online' || n.status === 'healthy' || n.status === 'leader' || n.status === 'follower').length
    const maintenance = nodes.filter(n => n.status === 'maintenance').length
    const degraded = nodes.filter(n => n.status === 'degraded').length
    const isolated = nodes.filter(n => n.status === 'isolated').length
    const offline = nodes.filter(n => n.status === 'offline').length
    const abnormal = degraded + isolated + offline
    const masters = nodes.filter(n => n.node_type === 'master').length
    const filers = nodes.filter(n => n.node_type === 'filer').length
    const vols = nodes.filter(n => n.node_type === 'volume').length
    return [
      {
        title: t('nav:items.nodes'),
        value: total,
        suffix: '',
        status: 'active',
        icon: <CloudServerOutlined />,
        loading,
        footer: (
          <Text type="secondary" style={{ fontSize: 12 }}>
            {t('common:master')} {masters} · {t('common:filer')} {filers} · {t('common:volume')} {vols}
          </Text>
        ),
      },
      {
        title: '运行中',
        value: online,
        suffix: '',
        status: 'active',
        icon: <ThunderboltOutlined />,
        loading,
        footer: <Text type="secondary" style={{ fontSize: 12 }}>健康节点</Text>,
      },
      {
        title: '维护中',
        value: maintenance,
        suffix: '',
        status: 'cordoned',
        icon: <HddOutlined />,
        loading,
        footer: <Text type="secondary" style={{ fontSize: 12 }}>主动维护</Text>,
      },
      {
        title: '异常',
        value: abnormal,
        suffix: '',
        status: abnormal > 0 ? 'unreachable' : 'active',
        icon: <ApiOutlined />,
        loading,
        footer: (
          <Text type="secondary" style={{ fontSize: 12 }}>
            {abnormal > 0 ? `降级${degraded} · 隔离${isolated} · 离线${offline}` : '全部正常'}
          </Text>
        ),
      },
    ]
  }, [nodes, loading, t])

  // ── Filtered data ──
  const filterBySearch = (list: NodeInfo[]) => {
    if (!search.trim()) return list
    const q = search.toLowerCase()
    return list.filter(
      n => n.id.toLowerCase().includes(q) || n.address.toLowerCase().includes(q),
    )
  }

  const masterNodes = useMemo(
    () => filterBySearch(nodes.filter(n => n.node_type === 'master')),
    [nodes, search],
  )
  const filerNodes = useMemo(
    () => filterBySearch(nodes.filter(n => n.node_type === 'filer')),
    [nodes, search],
  )
  const volumeNodes = useMemo(
    () => filterBySearch(nodes.filter(n => n.node_type === 'volume')),
    [nodes, search],
  )
  const allNodes = useMemo(() => filterBySearch(nodes), [nodes, search])

  // ── Table columns ──
  const columns = [
    {
      title: '节点ID',
      dataIndex: 'id',
      key: 'id',
      width: 140,
      render: (id: string) => <Text strong className="tabular-nums">{id}</Text>,
    },
    {
      title: t('common:type'),
      dataIndex: 'node_type',
      key: 'node_type',
      width: 100,
      render: (type: string) => {
        const color: Record<string, string> = { master: 'blue', filer: 'purple', volume: 'cyan' }
        const label: Record<string, string> = {
          master: t('common:master'),
          filer: t('common:filer'),
          volume: t('common:volume'),
        }
        return <Tag color={color[type] ?? 'default'}>{label[type] ?? type}</Tag>
      },
    },
    {
      title: t('common:address'),
      dataIndex: 'address',
      key: 'address',
      render: (address: string, record: NodeInfo) => (
        <Text type="secondary" className="font-mono" style={{ fontSize: 12 }}>
          {address}:{record.grpc_port}
        </Text>
      ),
    },
    {
      title: t('common:status'),
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: (status: string) => (
        <StatusTag kind="node" status={status} pulse={resolveNodeStatus(status).tag === 'success'} />
      ),
    },
    {
      title: 'Raft 角色',
      key: 'raft_role',
      width: 100,
      render: (_: unknown, record: NodeInfo) => {
        if (record.node_type !== 'master') return <Text type="secondary">-</Text>
        const role: 'leader' | 'follower' = record.is_leader ? 'leader' : 'follower'
        const palette = raftRolePalette[role]
        return (
          <Tag color={palette.tag} style={{ borderRadius: 6 }}>
            {role === 'leader' ? t('common:leader') : t('common:follower')}
          </Tag>
        )
      },
    },
    {
      title: t('common:cpu'),
      key: 'cpu',
      width: 130,
      render: (_: unknown, record: NodeInfo) => <ResourceBar value={record.cpu_usage} />,
    },
    {
      title: t('common:memory'),
      key: 'mem',
      width: 130,
      render: (_: unknown, record: NodeInfo) => <ResourceBar value={record.mem_usage} />,
    },
    {
      title: t('common:disk'),
      key: 'disk',
      width: 130,
      render: (_: unknown, record: NodeInfo) => <ResourceBar value={record.disk_usage} />,
    },
    {
      title: 'Volume',
      dataIndex: 'volume_count',
      key: 'volume_count',
      width: 80,
      render: (v: number) => <span className="tabular-nums">{v}</span>,
    },
    {
      title: '运行时间',
      dataIndex: 'uptime',
      key: 'uptime',
      width: 120,
      render: (uptime: number) => (
        <Text type="secondary" className="tabular-nums" style={{ fontSize: 12 }}>
          {formatUptime(uptime)}
        </Text>
      ),
    },
    {
      title: t('common:operation'),
      key: 'action',
      width: 140,
      fixed: 'right' as const,
      render: (_: unknown, record: NodeInfo) => (
        <Space size={0}>
          <Button type="text" size="small" icon={<EyeOutlined />} onClick={() => handleViewDetail(record)}>
            {t('common:view')}
          </Button>
          <PopconfirmNode>
            <Button
              type="text"
              size="small"
              danger
              icon={<DeleteOutlined />}
              onClick={() => handleDelete(record)}
            >
              {t('common:delete')}
            </Button>
          </PopconfirmNode>
        </Space>
      ),
    },
  ]

  const deviceColumns = [
    {
      title: '设备ID',
      dataIndex: 'device_id',
      key: 'device_id',
      width: 120,
      render: (id: string) => <Text className="font-mono" style={{ fontSize: 12 }}>{id}</Text>,
    },
    {
      title: t('common:type'),
      dataIndex: 'device_type',
      key: 'device_type',
      width: 110,
      render: (type: string) => {
        const typeMap: Record<string, string> = {
          local_file: '本地文件',
          spdk: 'SPDK',
          nvmeof: 'NVMe-oF',
        }
        return <Tag>{typeMap[type] || type}</Tag>
      },
    },
    {
      title: t('common:status'),
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => <StatusTag kind="device" status={status} />,
    },
    {
      title: '健康度',
      dataIndex: 'health',
      key: 'health',
      width: 90,
      render: (health?: string) => {
        if (!health) return <Text type="secondary">-</Text>
        const map: Record<string, string> = {
          healthy: 'active',
          warning: 'cordoned',
          critical: 'unreachable',
        }
        return <StatusTag kind="node" status={map[health] ?? health} label={health} />
      },
    },
    {
      title: '总容量',
      dataIndex: 'total_capacity',
      key: 'total_capacity',
      width: 100,
      render: (val: number) => <span className="tabular-nums">{formatBytes(val)}</span>,
    },
    {
      title: '已用空间',
      key: 'used',
      width: 170,
      render: (_: unknown, record: StorageDevice) => {
        const percent = record.total_capacity > 0
          ? (record.used_space / record.total_capacity) * 100
          : 0
        return <ResourceBar value={percent} suffix={formatBytes(record.used_space)} />
      },
    },
    {
      title: 'Volume',
      dataIndex: 'volume_count',
      key: 'volume_count',
      width: 80,
      render: (v?: number) => <span className="tabular-nums">{v ?? '-'}</span>,
    },
    {
      title: '最后检查',
      dataIndex: 'last_check',
      key: 'last_check',
      width: 150,
      render: (val?: string) => val ? <Text type="secondary" style={{ fontSize: 12 }}>{formatTime(val)}</Text> : <Text type="secondary">-</Text>,
    },
  ]

  const renderNodeTable = (list: NodeInfo[], emptyTitle: string, emptyDesc: string) =>
    loading && list.length === 0 ? (
      <SkeletonCard height={320} />
    ) : list.length === 0 ? (
      <EmptyState
        title={emptyTitle}
        description={emptyDesc}
        icon={<CloudServerOutlined style={{ color: 'var(--pf-color-text-tertiary)' }} />}
      />
    ) : (
      <Table
        columns={columns}
        dataSource={list}
        rowKey="id"
        size={density}
        pagination={{ pageSize: 10, showSizeChanger: true }}
        scroll={{ x: 1280 }}
      />
    )

  return (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            PowerFS 三层解耦架构：{t('common:master')} 管理调度 · {t('common:filer')} 管理元数据（Shard Raft） · {t('common:volume')} 管理实际数据存储。
          </Text>
        </div>
      </Card>

      {/* Page header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <div>
          <Title level={4} style={{ margin: 0 }}>{t('nav:items.nodes')}</Title>
          <Text type="secondary">PowerFS 集群节点状态与资源监控</Text>
        </div>
        <Space>
          <Tooltip title={t('common:refresh')}>
            <Button icon={<ReloadOutlined />} onClick={() => void loadNodes()} loading={loading} />
          </Tooltip>
          <Button type="primary" icon={<PlusOutlined />}>添加节点</Button>
        </Space>
      </div>

      {/* KPI summary */}
      <div style={{ marginBottom: 16 }}>
        <KpiBar items={kpiItems} />
      </div>

      <Card
        style={{ borderRadius: 12 }}
        styles={{ body: { padding: 16 } }}
        extra={
          <Space size={12} wrap>
            <Input
              allowClear
              size="middle"
              prefix={<SearchOutlined style={{ color: 'var(--pf-color-text-tertiary)' }} />}
              placeholder="搜索节点 ID / 地址"
              value={search}
              onChange={e => setSearch(e.target.value)}
              style={{ width: 220 }}
            />
            <Segmented
              size="small"
              value={density}
              onChange={v => setDensity(v as SizeType)}
              options={[
                { label: '紧凑', value: 'small' },
                { label: '默认', value: 'middle' },
                { label: '宽松', value: 'large' },
              ]}
            />
          </Space>
        }
      >
        <Tabs
          activeKey={activeTab}
          onChange={k => setActiveTab(k as NodeRoleTab)}
          items={[
            {
              key: 'all',
              label: (
                <span><InfoCircleOutlined /> {t('common:all')} ({allNodes.length})</span>
              ),
              children: renderNodeTable(allNodes, '暂无节点', '集群中尚未注册任何节点'),
            },
            {
              key: 'master',
              label: (
                <span><CloudServerOutlined /> {t('common:master')} ({masterNodes.length})</span>
              ),
              children: renderNodeTable(
                masterNodes,
                `暂无 ${t('common:master')} 节点`,
                '集群中尚未注册 Master 节点',
              ),
            },
            {
              key: 'filer',
              label: (
                <span><DashboardOutlined /> {t('common:filer')} ({filerNodes.length})</span>
              ),
              children: renderNodeTable(
                filerNodes,
                `暂无 ${t('common:filer')} 节点`,
                '集群中尚未注册 Filer 元数据节点',
              ),
            },
            {
              key: 'volume',
              label: (
                <span><DatabaseOutlined /> {t('common:volume')} ({volumeNodes.length})</span>
              ),
              children: renderNodeTable(
                volumeNodes,
                `暂无 ${t('common:volume')} 节点`,
                search ? '没有匹配的节点，请调整搜索条件' : '集群中尚未注册 Volume 存储节点',
              ),
            },
          ]}
        />
      </Card>

      {/* Detail Drawer */}
      <Drawer
        title="节点详情"
        open={showDetail}
        onClose={() => setShowDetail(false)}
        width={640}
        destroyOnClose
      >
        {selectedNode && (
          <Space direction="vertical" style={{ width: '100%', gap: 20 }}>
            {/* Header card */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 16, padding: 16, borderRadius: 12, background: 'var(--pf-gradient-brand-soft)' }}>
              <div style={{
                width: 56, height: 56, borderRadius: 14,
                background: 'var(--pf-gradient-brand)',
                color: '#fff', display: 'inline-flex',
                alignItems: 'center', justifyContent: 'center', fontSize: 26,
              }}>
                {selectedNode.node_type === 'master' ? <CloudServerOutlined /> :
                 selectedNode.node_type === 'filer' ? <DashboardOutlined /> : <DatabaseOutlined />}
              </div>
              <div style={{ flex: 1 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <Text strong style={{ fontSize: 18 }}>{selectedNode.id}</Text>
                  <StatusTag kind="node" status={selectedNode.status} pulse={resolveNodeStatus(selectedNode.status).tag === 'success'} />
                </div>
                <Text type="secondary" className="font-mono" style={{ fontSize: 12 }}>
                  {selectedNode.address}:{selectedNode.grpc_port}
                </Text>
              </div>
            </div>

            {/* Basic info */}
            <Descriptions
              title="基本信息"
              column={2}
              size="small"
              bordered
              items={[
                { key: 'type', label: '节点类型', children:
                  selectedNode.node_type === 'master' ? t('common:master') :
                  selectedNode.node_type === 'filer' ? t('common:filer') : t('common:volume') },
                { key: 'addr', label: 'gRPC 地址', children: `${selectedNode.address}:${selectedNode.grpc_port}` },
                { key: 'http', label: 'HTTP 端口', children: selectedNode.http_port },
                { key: 'uptime', label: '运行时间', children: formatUptime(selectedNode.uptime) },
                { key: 'vols', label: 'Volume 数量', children: `${selectedNode.volume_count} 个` },
                { key: 'devs', label: '设备数量', children: `${nodeDevices.length} 个` },
              ]}
            />

            {/* Resource usage */}
            <div>
              <Title level={5} style={{ margin: '0 0 12px' }}>资源使用</Title>
              <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
                <ResourceRow label="CPU 使用率" value={selectedNode.cpu_usage} />
                <ResourceRow label="内存使用率" value={selectedNode.mem_usage} />
                <ResourceRow label="磁盘使用率" value={selectedNode.disk_usage} />
              </Space>
            </div>

            {/* Network IO */}
            <Descriptions
              title="网络 IO"
              column={2}
              size="small"
              bordered
              items={[
                { key: 'rx', label: '接收', children: formatBytes(selectedNode.network_rx) },
                { key: 'tx', label: '发送', children: formatBytes(selectedNode.network_tx) },
              ]}
            />

            {/* Storage devices */}
            <div>
              <Title level={5} style={{ margin: '0 0 12px' }}>
                <Space><HddOutlined /> 存储设备 ({nodeDevices.length})</Space>
              </Title>
              {nodeDevices.length > 0 ? (
                <Table
                  columns={deviceColumns}
                  dataSource={nodeDevices}
                  rowKey="device_id"
                  pagination={{ pageSize: 5 }}
                  scroll={{ x: 900 }}
                  size="small"
                />
              ) : (
                <EmptyState
                  title="暂无存储设备"
                  description="该节点尚未挂载存储设备"
                  icon={<HddOutlined style={{ color: 'var(--pf-color-text-tertiary)' }} />}
                />
              )}
            </div>
          </Space>
        )}
      </Drawer>

      {/* Delete confirm */}
      <Modal
        title="确认删除节点"
        open={showDeleteConfirm}
        onCancel={() => setShowDeleteConfirm(false)}
        onOk={confirmDelete}
        okText="确认删除"
        cancelText={t('common:cancel')}
        okButtonProps={{ danger: true }}
      >
        <p>确定要删除节点 <strong>{selectedNode?.id}</strong> 吗？</p>
        <p style={{ color: 'var(--pf-color-text-tertiary)', fontSize: 12 }}>
          该操作仅在 Monitor 视图移除节点；下一次心跳会重新加入节点。若需彻底下线请先执行 Master drain。
        </p>
      </Modal>
    </div>
  )
}

function PopconfirmNode({ children }: { children: React.ReactElement }) {
  return children
}

/** Compact resource bar with percent label and threshold-based color. */
function ResourceBar({ value, suffix }: { value: number; suffix?: string }) {
  const color = value > 80 ? '#ff4d4f' : value > 60 ? '#faad14' : '#52c41a'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <Progress
        percent={Math.min(100, value)}
        size="small"
        strokeColor={color}
        showInfo={false}
        style={{ flex: 1, minWidth: 60, margin: 0 }}
      />
      <span className="tabular-nums" style={{ fontSize: 12, minWidth: 42, textAlign: 'right' }}>
        {suffix ?? formatPercent(value)}
      </span>
    </div>
  )
}

/** Labeled resource row for the detail drawer. */
function ResourceRow({ label, value }: { label: string; value: number }) {
  const color = value > 80 ? '#ff4d4f' : value > 60 ? '#faad14' : '#52c41a'
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
        <Text type="secondary" style={{ fontSize: 13 }}>{label}</Text>
        <Text strong className="tabular-nums" style={{ color }}>{formatPercent(value)}</Text>
      </div>
      <Progress percent={Math.min(100, value)} strokeColor={color} showInfo={false} />
    </div>
  )
}

export default Nodes
