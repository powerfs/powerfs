import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card, Table, Tag, Statistic, Row, Col, Spin, message, Tooltip, Empty, Space,
  Typography, Select, Button, Popconfirm, Progress,
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
  ArrowRightOutlined,
  HeartOutlined,
  ApiOutlined,
  ThunderboltFilled,
  NodeIndexOutlined,
  ClockCircleOutlined,
  ApartmentOutlined,
} from '@ant-design/icons'
import type { FilerStatus, FilerNode } from '@/types'
import {
  getFilerNodes, getFilerNodeStatus, triggerFilerNodeBalancer,
} from '@/services/api'
import { isNodeAlive } from '@/utils/format'

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
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({})

  // ── 选中节点状态 ──
  const [status, setStatus] = useState<FilerStatus | null>(null)
  const [statusLoading, setStatusLoading] = useState(false)

  const navigate = useNavigate()

  // ── 节点列表加载 (10s 轮询) ──
  const loadNodes = useCallback(async () => {
    setNodesLoading(true)
    try {
      const data = await getFilerNodes()
      setNodes(data)
      // 自动选中第一个在线节点 (或首个节点)
      if (data.length > 0 && !data.some(n => n.node_id === selectedNodeId)) {
        const online = data.find(n => isNodeAlive(n.heartbeat_status))
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
      setStatus(null)
    } finally {
      setStatusLoading(false)
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

  // ── 节点操作 ──
  const handleTriggerBalance = async (nodeId: string) => {
    setActionLoading(prev => ({ ...prev, [`trigger-${nodeId}`]: true }))
    try {
      await triggerFilerNodeBalancer(nodeId)
      message.success(`节点 ${nodeId} 已触发 rebalance`)
    } catch {
      message.error(`节点 ${nodeId} 触发 rebalance 失败`)
    } finally {
      setActionLoading(prev => ({ ...prev, [`trigger-${nodeId}`]: false }))
    }
  }

  const handleToggleBalancer = (node: FilerNode) => {
    message.info(`请到「分片均衡」页面操作节点 ${node.node_id} 的 Balancer`)
    navigate('/shard-balancing')
  }

  // ── KPI 统计 ──
  const onlineCount = nodes.filter(n => isNodeAlive(n.heartbeat_status)).length
  const offlineCount = nodes.length - onlineCount
  const totalLeaders = nodes.reduce((sum, n) => sum + n.leader_count, 0)

  const handleViewNodeStatus = (nodeId: string) => {
    setSelectedNodeId(nodeId)
  }

  // ── 节点列表列定义 ──
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
      render: (hbStatus: string, r: FilerNode) => {
        const online = isNodeAlive(hbStatus)
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
          <Button
            type={selectedNodeId === r.node_id ? 'primary' : 'link'}
            size="small"
            onClick={() => handleViewNodeStatus(r.node_id)}
          >
            详情
          </Button>
          <Popconfirm
            title={`触发节点 ${r.node_id} 的 rebalance 检查?`}
            onConfirm={() => handleTriggerBalance(r.node_id)}
            disabled={!isNodeAlive(r.heartbeat_status)}
          >
            <Button
              type="link"
              size="small"
              icon={<ThunderboltOutlined />}
              loading={actionLoading[`trigger-${r.node_id}`]}
              disabled={!isNodeAlive(r.heartbeat_status)}
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

  return (
    <div>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <CloudServerOutlined style={{ fontSize: 24, color: 'var(--pf-color-primary)' }} />
          <Typography.Title level={4} style={{ margin: 0 }}>Filer 状态</Typography.Title>
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

      {/* ── KPI 统计 ── */}
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
      </Spin>

      {/* ── 节点列表 ── */}
      <Card
        title="Filer 节点列表"
        size="small"
        style={{ marginBottom: 16 }}
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
            rowClassName={(r) => !isNodeAlive(r.heartbeat_status) ? 'pf-row-warning' : ''}
          />
        ) : (
          <Empty description={nodesLoading ? '加载中...' : '集群暂无 Filer 节点'} />
        )}
      </Card>

      {/* ── 选中节点状态 ── */}
      <Card
        title={
          <Space>
            <DatabaseOutlined />
            <span>节点状态</span>
            {selectedNodeId && (
              <Tag color={isNodeAlive(nodes.find(n => n.node_id === selectedNodeId)?.heartbeat_status) ? 'success' : 'error'}>
                {selectedNodeId}
              </Tag>
            )}
          </Space>
        }
        size="small"
        extra={
          <Space>
            <Select
              size="small"
              style={{ width: 240 }}
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
              <Tooltip title="刷新">
                <Button icon={<ReloadOutlined />} onClick={() => loadStatus(selectedNodeId)} size="small">刷新</Button>
              </Tooltip>
            )}
          </Space>
        }
      >
        <Spin spinning={statusLoading}>
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
                  <TypographyLink onClick={() => navigate('/s3')} style={{ fontSize: 12 }}>
                    S3 管理 <ArrowRightOutlined />
                  </TypographyLink>
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
    </div>
  )
}

export default Filer
