import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card, Table, Tag, Statistic, Row, Col, Spin, message, Tooltip, Empty, Space,
  Typography, Descriptions, Alert, Progress, Tabs, Button, Select, Popconfirm,
} from 'antd'
import {
  CloudServerOutlined,
  DatabaseOutlined,
  FileOutlined,
  FolderOutlined,
  ThunderboltOutlined,
  ReloadOutlined,
  InfoCircleOutlined,
  WarningOutlined,
  SafetyCertificateOutlined,
  ArrowRightOutlined,
  HeartOutlined,
  ApiOutlined,
  ThunderboltFilled,
  NodeIndexOutlined,
  ClockCircleOutlined,
  ApartmentOutlined,
} from '@ant-design/icons'
import type { FilerStatus, ConflictStats, ConflictRecord, FilerNode } from '@/types'
import {
  getFilerNodes, getFilerNodeStatus, getConflictStats, getConflicts,
  triggerFilerNodeBalancer,
} from '@/services/api'

const { Text, Link: TypographyLink } = Typography

function formatUptime(secs: number): string {
  if (secs < 60) return `${secs}s`
  if (secs < 3600) return `${Math.floor(secs / 60)}m`
  if (secs < 86400) return `${Math.floor(secs / 3600)}h`
  return `${Math.floor(secs / 86400)}d`
}

function Filer() {
  // ── 节点列表 ──
  const [nodes, setNodes] = useState<FilerNode[]>([])
  const [nodesLoading, setNodesLoading] = useState(true)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState('nodes')
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({})

  // ── 选中节点状态 ──
  const [status, setStatus] = useState<FilerStatus | null>(null)
  const [statusLoading, setStatusLoading] = useState(false)

  // ── 冲突健康 ──
  const [conflictStats, setConflictStats] = useState<ConflictStats | null>(null)
  const [recentConflicts, setRecentConflicts] = useState<ConflictRecord[]>([])
  const [conflictLoading, setConflictLoading] = useState(false)

  const navigate = useNavigate()

  // ── 节点列表加载 (10s 轮询, 见 docs/filer-redesign-plan.md 决策 3) ──
  const loadNodes = useCallback(async () => {
    setNodesLoading(true)
    try {
      const data = await getFilerNodes()
      setNodes(data)
      // 自动选中第一个在线节点 (或首个节点)
      if (data.length > 0 && !data.some(n => n.node_id === selectedNodeId)) {
        const online = data.find(n => n.heartbeat_status === 'online')
        setSelectedNodeId((online ?? data[0]).node_id)
      }
    } catch (error) {
      console.error('Failed to load filer nodes:', error)
      message.error('加载 Filer 节点列表失败')
    } finally {
      setNodesLoading(false)
    }
  }, [selectedNodeId])

  // ── 选中节点状态加载 ──
  const loadStatus = useCallback(async (nodeId: string) => {
    setStatusLoading(true)
    try {
      const data = await getFilerNodeStatus(nodeId)
      setStatus(data)
    } catch (error) {
      console.error('Failed to load filer node status:', error)
      message.error('加载 Filer 节点状态失败')
      setStatus(null)
    } finally {
      setStatusLoading(false)
    }
  }, [])

  // ── 冲突健康加载 ──
  const loadConflictHealth = useCallback(async () => {
    setConflictLoading(true)
    try {
      const [stats, list] = await Promise.all([
        getConflictStats(),
        getConflicts({ unresolved_only: false }),
      ])
      setConflictStats(stats)
      setRecentConflicts(list.slice(0, 10))
    } catch (error) {
      console.warn('Failed to load conflict health (CRDT deprecated, may be empty):', error)
    } finally {
      setConflictLoading(false)
    }
  }, [])

  useEffect(() => {
    loadNodes()
    const timer = setInterval(loadNodes, 10000)
    return () => clearInterval(timer)
  }, [loadNodes])

  // 选中节点变化时加载状态
  useEffect(() => {
    if (selectedNodeId) {
      loadStatus(selectedNodeId)
    } else {
      setStatus(null)
    }
  }, [selectedNodeId, loadStatus])

  // 切到冲突健康 Tab 时加载
  const handleTabChange = (key: string) => {
    setActiveTab(key)
    if (key === 'health' && !conflictStats) {
      loadConflictHealth()
    }
  }

  // ── 节点操作 ──
  const handleTriggerBalance = async (nodeId: string) => {
    setActionLoading(prev => ({ ...prev, [`trigger-${nodeId}`]: true }))
    try {
      await triggerFilerNodeBalancer(nodeId)
      message.success(`节点 ${nodeId} 已触发 rebalance`)
    } catch (error) {
      message.error(`节点 ${nodeId} 触发 rebalance 失败`)
    } finally {
      setActionLoading(prev => ({ ...prev, [`trigger-${nodeId}`]: false }))
    }
  }

  const handleToggleBalancer = (node: FilerNode) => {
    // Balancer 的 start/stop/trigger 需要展示运行状态, 放在「分片均衡」页面操作更合适。
    // 这里引导用户过去, 并携带 node_id 上下文 (Phase B 可通过 query param 预选节点)。
    message.info(`请到「分片均衡」页面操作节点 ${node.node_id} 的 Balancer`)
    navigate('/shard-balancing')
  }

  // ── KPI 统计 ──
  const onlineCount = nodes.filter(n => n.heartbeat_status === 'online').length
  const offlineCount = nodes.length - onlineCount
  const totalLeaders = nodes.reduce((sum, n) => sum + n.leader_count, 0)

  const handleViewNodeStatus = (nodeId: string) => {
    setSelectedNodeId(nodeId)
    setActiveTab('status')
  }

  // ═══════════ Tab 1: 节点管理 ═══════════
  const nodeColumns = [
    {
      title: '节点 ID',
      dataIndex: 'node_id',
      key: 'node_id',
      width: 140,
      render: (id: string) => <Text strong>{id}</Text>,
    },
    {
      title: '地址',
      key: 'address',
      width: 200,
      render: (_: unknown, r: FilerNode) => (
        <Text code style={{ fontSize: 12 }}>{r.address}:{r.http_port}</Text>
      ),
    },
    {
      title: '心跳状态',
      dataIndex: 'heartbeat_status',
      key: 'heartbeat_status',
      width: 110,
      render: (status: string, r: FilerNode) => {
        const online = status === 'online'
        return (
          <Tooltip title={online ? `${r.last_seen_ago_secs}s 前心跳` : '心跳超时 (>30s)'}>
            <Tag color={online ? 'success' : 'error'} icon={<HeartOutlined />}>
              {online ? '在线' : '离线'}
            </Tag>
          </Tooltip>
        )
      },
    },
    {
      title: 'Leader / Shard',
      key: 'leader_shards',
      width: 130,
      sorter: (a: FilerNode, b: FilerNode) => a.leader_count - b.leader_count,
      render: (_: unknown, r: FilerNode) => (
        <Space split={<Text type="secondary">/</Text>}>
          <span><ThunderboltFilled style={{ color: '#faad14' }} /> {r.leader_count}</span>
          <span><DatabaseOutlined /> {r.total_shards}</span>
        </Space>
      ),
    },
    {
      title: '负载',
      key: 'load',
      width: 180,
      render: (_: unknown, r: FilerNode) => (
        <Space direction="vertical" size={0} style={{ width: '100%' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12 }}>
            <Text type="secondary" style={{ width: 32 }}>CPU</Text>
            <Progress percent={Math.round(r.cpu_usage)} size="small" strokeColor={r.cpu_usage > 80 ? '#ff4d4f' : '#1677ff'} showInfo={false} style={{ flex: 1, margin: 0 }} />
            <Text style={{ width: 36, textAlign: 'right' }}>{r.cpu_usage.toFixed(1)}%</Text>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12 }}>
            <Text type="secondary" style={{ width: 32 }}>Mem</Text>
            <Progress percent={Math.round(r.mem_usage)} size="small" strokeColor={r.mem_usage > 85 ? '#ff4d4f' : '#52c41a'} showInfo={false} style={{ flex: 1, margin: 0 }} />
            <Text style={{ width: 36, textAlign: 'right' }}>{r.mem_usage.toFixed(1)}%</Text>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12 }}>
            <Text type="secondary" style={{ width: 32 }}>Disk</Text>
            <Progress percent={Math.round(r.disk_usage)} size="small" strokeColor={r.disk_usage > 90 ? '#ff4d4f' : '#faad14'} showInfo={false} style={{ flex: 1, margin: 0 }} />
            <Text style={{ width: 36, textAlign: 'right' }}>{r.disk_usage.toFixed(1)}%</Text>
          </div>
        </Space>
      ),
    },
    {
      title: '运行时长',
      dataIndex: 'uptime',
      key: 'uptime',
      width: 90,
      render: (uptime: number) => (
        <Tooltip title={`${uptime} 秒`}>
          <Space size={4}>
            <ClockCircleOutlined style={{ color: 'var(--pf-color-secondary)' }} />
            <Text style={{ fontSize: 12 }}>{formatUptime(uptime)}</Text>
          </Space>
        </Tooltip>
      ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 200,
      render: (_: unknown, r: FilerNode) => (
        <Space size={4}>
          <Button type="link" size="small" onClick={() => handleViewNodeStatus(r.node_id)}>
            详情
          </Button>
          <Popconfirm
            title={`触发节点 ${r.node_id} 的 rebalance 检查?`}
            onConfirm={() => handleTriggerBalance(r.node_id)}
            disabled={r.heartbeat_status !== 'online'}
          >
            <Button
              type="link"
              size="small"
              icon={<ThunderboltOutlined />}
              loading={actionLoading[`trigger-${r.node_id}`]}
              disabled={r.heartbeat_status !== 'online'}
            >
              Rebalance
            </Button>
          </Popconfirm>
          <Button
            type="link"
            size="small"
            icon={<ApiOutlined />}
            onClick={() => handleToggleBalancer(r)}
          >
            Balancer
          </Button>
        </Space>
      ),
    },
  ]

  const nodesTab = (
    <Spin spinning={nodesLoading}>
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={12} sm={8} md={6}>
          <Card><Statistic title="节点总数" value={nodes.length} prefix={<NodeIndexOutlined />} /></Card>
        </Col>
        <Col xs={12} sm={8} md={6}>
          <Card><Statistic title="在线" value={onlineCount} valueStyle={{ color: 'var(--pf-color-success)' }} prefix={<HeartOutlined />} /></Card>
        </Col>
        <Col xs={12} sm={8} md={6}>
          <Card><Statistic title="离线" value={offlineCount} valueStyle={{ color: offlineCount > 0 ? 'var(--pf-color-error)' : undefined }} prefix={<WarningOutlined />} /></Card>
        </Col>
        <Col xs={12} sm={8} md={6}>
          <Card><Statistic title="Leader 总数" value={totalLeaders} prefix={<ThunderboltOutlined />} /></Card>
        </Col>
      </Row>

      <Card
        title="Filer 节点列表"
        size="small"
        extra={
          <Tooltip title="刷新">
            <Button icon={<ReloadOutlined />} onClick={loadNodes} size="small">刷新</Button>
          </Tooltip>
        }
      >
        {nodes.length > 0 ? (
          <Table
            columns={nodeColumns}
            dataSource={nodes}
            rowKey="node_id"
            pagination={false}
            size="middle"
            rowClassName={(r) => r.heartbeat_status !== 'online' ? 'pf-row-warning' : ''}
          />
        ) : (
          <Empty description={nodesLoading ? '加载中...' : '集群暂无 Filer 节点'} />
        )}
      </Card>

      <Card size="small" style={{ marginTop: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            节点列表合并 master 注册视角 (gRPC ListFilers) 与心跳视角 (metric_store)。
            心跳超时 30s 的节点会被标记为「离线」。如需管理分片均衡, 请到{' '}
            <TypographyLink onClick={() => navigate('/shard-balancing')}>分片均衡 <ArrowRightOutlined /></TypographyLink>
            {' '}页面。
          </Text>
        </div>
      </Card>
    </Spin>
  )

  // ═══════════ Tab 2: 节点状态 ═══════════
  const statusTab = (
    <Spin spinning={statusLoading}>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <Text type="secondary">选择节点:</Text>
          <Select
            style={{ width: 260 }}
            placeholder="选择 Filer 节点"
            value={selectedNodeId ?? undefined}
            onChange={(v) => setSelectedNodeId(v)}
            options={nodes.map(n => ({
              value: n.node_id,
              label: `${n.node_id} (${n.address}:${n.http_port})`,
            }))}
            notFoundContent="暂无节点"
          />
          {selectedNodeId && (
            <Tag color={nodes.find(n => n.node_id === selectedNodeId)?.heartbeat_status === 'online' ? 'success' : 'error'}>
              {nodes.find(n => n.node_id === selectedNodeId)?.heartbeat_status === 'online' ? '在线' : '离线'}
            </Tag>
          )}
        </Space>
        {selectedNodeId && (
          <Tooltip title="刷新">
            <Button icon={<ReloadOutlined />} onClick={() => loadStatus(selectedNodeId)} size="small">刷新</Button>
          </Tooltip>
        )}
      </div>

      {!selectedNodeId ? (
        <Empty description="请选择一个 Filer 节点" />
      ) : !status ? (
        <Empty description={statusLoading ? '加载中...' : '无法加载节点状态'} />
      ) : (
        <>
          <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
            <Col xs={12} sm={8} md={4}>
              <Card><Statistic title="分片总数" value={status.shard_count} prefix={<DatabaseOutlined />} /></Card>
            </Col>
            <Col xs={12} sm={8} md={4}>
              <Card><Statistic title="Leader 分片" value={status.leader_count} valueStyle={{ color: 'var(--pf-color-success)' }} prefix={<ThunderboltOutlined />} /></Card>
            </Col>
            <Col xs={12} sm={8} md={4}>
              <Card><Statistic title="Inode 总数" value={status.total_inodes} prefix={<FileOutlined />} /></Card>
            </Col>
            <Col xs={12} sm={8} md={4}>
              <Card><Statistic title="文件数" value={status.total_files} prefix={<FileOutlined />} /></Card>
            </Col>
            <Col xs={12} sm={8} md={4}>
              <Card><Statistic title="目录数" value={status.total_dirs} prefix={<FolderOutlined />} /></Card>
            </Col>
            <Col xs={12} sm={8} md={4}>
              <Card><Statistic title="Bucket 数" value={status.buckets?.length ?? 0} prefix={<DatabaseOutlined />} /></Card>
            </Col>
          </Row>

          <Card
            title="Bucket 列表"
            size="small"
            extra={
              <Space>
                <Tag color="blue">节点 {selectedNodeId}</Tag>
                <TypographyLink onClick={() => navigate('/s3')} style={{ fontSize: 12 }}>
                  S3 管理 <ArrowRightOutlined />
                </TypographyLink>
              </Space>
            }
          >
            {status.buckets && status.buckets.length > 0 ? (
              <Table
                columns={[
                  {
                    title: 'Bucket 名称',
                    dataIndex: 'name',
                    key: 'name',
                    render: (name: string) => (
                      <Space>
                        <DatabaseOutlined style={{ color: 'var(--pf-color-primary)' }} />
                        <Text strong>{name}</Text>
                      </Space>
                    ),
                  },
                  { title: '状态', key: 'status', width: 120, render: () => <Tag color="success">活跃</Tag> },
                ]}
                dataSource={(status.buckets ?? []).map(name => ({ key: name, name }))}
                pagination={{ pageSize: 10 }}
                size="middle"
              />
            ) : (
              <Empty description="暂无 Bucket" />
            )}
          </Card>
        </>
      )}
    </Spin>
  )

  // ═══════════ Tab 3: 冲突健康 ═══════════
  const healthTab = (
    <Card
      size="small"
      title={
        <Space>
          {conflictStats && conflictStats.unresolved_count > 0
            ? <WarningOutlined style={{ color: 'var(--pf-color-warning)' }} />
            : <SafetyCertificateOutlined style={{ color: 'var(--pf-color-success)' }} />}
          <span>CRDT 冲突健康指示器</span>
          <Tag color="blue" style={{ fontSize: 11 }}>CRDT 已弃用</Tag>
        </Space>
      }
      extra={
        <Space>
          <Tooltip title="刷新">
            <ReloadOutlined
              onClick={loadConflictHealth}
              style={{ fontSize: 14, cursor: 'pointer', color: 'var(--pf-color-primary)' }}
            />
          </Tooltip>
          <TypographyLink onClick={() => navigate('/conflicts')} style={{ fontSize: 12 }}>
            完整冲突管理 <ArrowRightOutlined />
          </TypographyLink>
        </Space>
      }
    >
      {conflictLoading && !conflictStats ? (
        <Empty description="加载中..." />
      ) : conflictStats ? (
        <>
          {conflictStats.total_count === 0 ? (
            <Alert
              type="success"
              showIcon
              icon={<SafetyCertificateOutlined />}
              message="CRDT 状态健康"
              description="集群当前不存在任何冲突记录，Filer Raft 一致性运行正常。"
              style={{ marginBottom: 16 }}
            />
          ) : (
            <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
              <Col xs={12} sm={8} md={4}>
                <Card size="small" variant="outlined">
                  <Statistic title="累计冲突" value={conflictStats.total_count} valueStyle={{ fontSize: 16 }} prefix={<WarningOutlined />} />
                </Card>
              </Col>
              <Col xs={12} sm={8} md={4}>
                <Card size="small" variant="outlined">
                  <Statistic
                    title="待处理"
                    value={conflictStats.unresolved_count}
                    valueStyle={{ color: conflictStats.unresolved_count > 0 ? 'var(--pf-color-warning)' : 'var(--pf-color-success)', fontSize: 16 }}
                    prefix={<WarningOutlined />}
                    suffix={conflictStats.unresolved_count > 0 ? '' : '✓'}
                  />
                </Card>
              </Col>
              <Col xs={12} sm={8} md={4}>
                <Card size="small" variant="outlined">
                  <Statistic title="已解决" value={conflictStats.resolved_count} valueStyle={{ color: 'var(--pf-color-success)', fontSize: 16 }} prefix={<SafetyCertificateOutlined />} />
                </Card>
              </Col>
              <Col xs={24} sm={24} md={12}>
                <Card size="small" variant="outlined">
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    解决率 ({conflictStats.total_count === 0 ? '—' : `${Math.round((conflictStats.resolved_count / conflictStats.total_count) * 100)}%`})
                  </Text>
                  <Progress
                    percent={conflictStats.total_count === 0 ? 100 : Math.round((conflictStats.resolved_count / conflictStats.total_count) * 100)}
                    size="small"
                    strokeColor="var(--pf-color-success)"
                    showInfo={false}
                    style={{ marginTop: 6 }}
                  />
                </Card>
              </Col>
            </Row>
          )}

          {recentConflicts.length > 0 && (
            <>
              <div style={{ fontWeight: 500, marginBottom: 8 }}>最近 {recentConflicts.length} 条冲突记录</div>
              <Table
                size="small"
                rowKey="id"
                pagination={false}
                dataSource={recentConflicts}
                columns={[
                  { title: '冲突 ID', dataIndex: 'id', key: 'id', width: 130, render: (id: string) => id.slice(0, 12) + '…' },
                  {
                    title: '类型', dataIndex: 'conflict_type', key: 'type', width: 100,
                    render: (t: number) => {
                      const typeMap: Record<number, { label: string; color: string }> = {
                        0: { label: 'create-create', color: 'orange' },
                        1: { label: 'write-write', color: 'red' },
                        2: { label: 'write-unlink', color: 'volcano' },
                        3: { label: 'delete-create', color: 'purple' },
                        4: { label: 'rename', color: 'cyan' },
                      }
                      const m = typeMap[t] ?? { label: `type-${t}`, color: 'default' }
                      return <Tag color={m.color}>{m.label}</Tag>
                    },
                  },
                  { title: '路径', dataIndex: 'dir_path', key: 'path', render: (p: string) => p || <Text type="secondary">/</Text> },
                  {
                    title: '状态', key: 'st', width: 90,
                    render: (_: unknown, r: ConflictRecord) =>
                      r.resolved ? <Tag color="success">resolved</Tag> : <Tag color="warning">pending</Tag>,
                  },
                ]}
              />
            </>
          )}
        </>
      ) : (
        <Empty description="无法加载冲突统计" />
      )}
    </Card>
  )

  // ═══════════ Tab 4: 常见问题 ═══════════
  const faqTab = (
    <Card title="常见问题" size="small">
      <Descriptions column={1} size="small">
        <Descriptions.Item label="什么是 Filer？">
          Filer 是 PowerFS 的文件系统元数据管理组件，负责管理文件和目录的元数据（如文件名、大小、权限、时间戳等），处理文件系统的创建、读取、更新、删除操作。
        </Descriptions.Item>
        <Descriptions.Item label="什么是分片（Shard）？">
          Filer 将元数据按 Inode 范围分片存储，每个分片由一组节点管理。分片可以分散元数据负载，提高并发处理能力。
        </Descriptions.Item>
        <Descriptions.Item label="什么是 Leader 分片？">
          每个分片有一个 Leader 节点负责处理写入请求，其他节点作为 Follower 同步数据。Leader 负责决策，Follower 提供冗余。
        </Descriptions.Item>
        <Descriptions.Item label="什么是 Inode？">
          Inode 是文件系统中用于描述文件或目录属性的数据结构，包含文件大小、权限、所有者、时间戳等信息。每个文件/目录对应一个唯一的 Inode。
        </Descriptions.Item>
        <Descriptions.Item label="什么是 Bucket？">
          Bucket 是 S3 兼容接口中的存储容器概念，类似于文件系统中的顶级目录。每个 Bucket 可以存储大量对象。
        </Descriptions.Item>
        <Descriptions.Item label="心跳状态如何判定？">
          Monitor 通过 metric_store 跟踪每个 Filer 节点的心跳。若超过 30 秒未收到心跳，节点会被标记为「离线」。这是真实健康状态，区别于 master 注册视角的静态 is_healthy 值。
        </Descriptions.Item>
      </Descriptions>
    </Card>
  )

  return (
    <div>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <CloudServerOutlined style={{ fontSize: 24, color: 'var(--pf-color-primary)' }} />
          <Typography.Title level={4} style={{ margin: 0 }}>Filer 管理</Typography.Title>
          {nodes.length > 0 && (
            <Tag color="blue" icon={<ApartmentOutlined />}>{nodes.length} 个节点</Tag>
          )}
        </Space>
        <Tooltip title="刷新节点列表">
          <Button icon={<ReloadOutlined />} onClick={loadNodes} size="small">刷新</Button>
        </Tooltip>
      </div>

      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            Filer 是 PowerFS 的元数据管理组件，通过 Raft 协议保证多副本一致性。
            所有 Filer admin 操作由 Monitor 统一代理（前端不直连 filer），要求 admin 权限。
          </Text>
        </div>
      </Card>

      <Tabs
        activeKey={activeTab}
        onChange={handleTabChange}
        size="large"
        items={[
          {
            key: 'nodes',
            label: (
              <span>
                <ApartmentOutlined style={{ marginRight: 6 }} />
                节点管理
                {offlineCount > 0 && <Tag color="error" style={{ marginLeft: 8, fontSize: 11 }}>{offlineCount} 离线</Tag>}
              </span>
            ),
            children: nodesTab,
          },
          {
            key: 'status',
            label: (
              <span>
                <DatabaseOutlined style={{ marginRight: 6 }} />
                节点状态
              </span>
            ),
            children: statusTab,
          },
          {
            key: 'health',
            label: (
              <span>
                <SafetyCertificateOutlined style={{ marginRight: 6 }} />
                冲突健康
                {conflictStats && conflictStats.unresolved_count > 0 && (
                  <Tag color="warning" style={{ marginLeft: 8, fontSize: 11 }}>{conflictStats.unresolved_count}</Tag>
                )}
              </span>
            ),
            children: healthTab,
          },
          {
            key: 'faq',
            label: '常见问题',
            children: faqTab,
          },
        ]}
      />
    </div>
  )
}

export default Filer
