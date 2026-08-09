import { useEffect, useMemo, useState } from 'react'
import {
  Card,
  Tree,
  Tag,
  Spin,
  Row,
  Col,
  Typography,
  Statistic,
  Progress,
  Tooltip,
  Segmented,
  Drawer,
  Descriptions,
  Space,
  Button,
} from 'antd'
import {
  DatabaseOutlined,
  HddOutlined,
  ReloadOutlined,
  CheckCircleFilled,
  WarningFilled,
  InfoCircleOutlined,
  CloudServerOutlined,
  CrownOutlined,
  TeamOutlined,
  DashboardOutlined,
  ClusterOutlined,
} from '@ant-design/icons'
import ReactFlow, {
  Background,
  BackgroundVariant,
  Controls,
  MarkerType,
  MiniMap,
  type Edge,
  type Node as FlowNode,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { useTranslation } from 'react-i18next'
import type { TopologyData, VolumeServerInfo, NodeInfo, FilerNodeInfo } from '@/types'
import { getTopology } from '@/services/api'
import { formatBytes } from '@/utils/format'

const { Title, Text } = Typography

type TreeNode = {
  key: string
  title: React.ReactNode
  children?: TreeNode[]
}

type ViewMode = 'tree' | 'graph'
type NodeDrawerState =
  | { kind: 'master'; data: NodeInfo }
  | { kind: 'filer'; data: FilerNodeInfo }
  | { kind: 'volume'; data: VolumeServerInfo }
  | null

function ClusterTopology() {
  const { t } = useTranslation(['common', 'nav'])
  const [topology, setTopology] = useState<TopologyData | null>(null)
  const [loading, setLoading] = useState(false)
  const [view, setView] = useState<ViewMode>('graph')
  const [drawer, setDrawer] = useState<NodeDrawerState>(null)

  const loadTopology = async () => {
    setLoading(true)
    try {
      const data = await getTopology()
      setTopology(data)
    } catch (e) {
      console.error('Failed to load topology:', e)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadTopology()
    const interval = setInterval(loadTopology, 15000)
    return () => clearInterval(interval)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const buildTree = (data: TopologyData): TreeNode[] => {
    const root: TreeNode = {
      key: 'root',
      title: (
        <span>
          <DatabaseOutlined style={{ marginRight: 8 }} />
          PowerFS Cluster
          <Tag color="blue" style={{ marginLeft: 8 }}>
            {data.masters.length} {t('common:master')} · {data.filers.length} {t('common:filer')} ·{' '}
            {data.volume_servers.length} {t('common:volume')}
          </Tag>
        </span>
      ),
      children: [],
    }

    const masterGroup: TreeNode = {
      key: 'masters',
      title: (
        <span>
          <CloudServerOutlined style={{ marginRight: 8 }} />
          Master Nodes
          <Tag color="green" style={{ marginLeft: 8 }}>
            {data.masters.length}
          </Tag>
        </span>
      ),
      children: data.masters.map(m => ({
        key: `master-${m.id}`,
        title: (
          <Tooltip title={`${m.address}:${m.grpc_port}`}>
            <span
              onClick={() => setDrawer({ kind: 'master', data: m })}
              style={{ cursor: 'pointer' }}
            >
              {m.is_leader ? (
                <Tag color="gold">LEADER</Tag>
              ) : (
                <Tag color="blue">FOLLOWER</Tag>
              )}
              <Text strong>{m.id}</Text>
              <Text type="secondary" style={{ marginLeft: 8 }}>
                {m.status}
              </Text>
              <Text type="secondary" style={{ marginLeft: 12 }}>
                {t('common:cpu')} {m.cpu_usage.toFixed(0)}% · {t('common:memory')}{' '}
                {m.mem_usage.toFixed(0)}%
              </Text>
            </span>
          </Tooltip>
        ),
      })),
    }

    const filerGroup: TreeNode = {
      key: 'filers',
      title: (
        <span>
          <CloudServerOutlined style={{ marginRight: 8 }} />
          Filer Nodes (Metadata)
          <Tag color="purple" style={{ marginLeft: 8 }}>
            {data.filers.length}
          </Tag>
        </span>
      ),
      children: data.filers.map(f => ({
        key: `filer-${f.node_id}`,
        title: (
          <Tooltip title={`${f.address}:${f.grpc_port}`}>
            <span
              onClick={() => setDrawer({ kind: 'filer', data: f })}
              style={{ cursor: 'pointer' }}
            >
              {f.is_healthy ? <Tag color="success">HEALTHY</Tag> : <Tag color="error">UNHEALTHY</Tag>}
              <Text strong>{f.node_id}</Text>
              <Text type="secondary" style={{ marginLeft: 8 }}>
                {f.total_shards} shards · {f.leader_count} leader
              </Text>
            </span>
          </Tooltip>
        ),
      })),
    }

    const volumeGroup: TreeNode = {
      key: 'volumes',
      title: (
        <span>
          <HddOutlined style={{ marginRight: 8 }} />
          Volume Servers (Data)
          <Tag color="cyan" style={{ marginLeft: 8 }}>
            {data.volume_servers.length}
          </Tag>
        </span>
      ),
      children: data.volume_servers.map(vs => buildVolumeServerNode(vs)),
    }

    root.children = [masterGroup, filerGroup, volumeGroup]
    return [root]
  }

  const buildVolumeServerNode = (vs: VolumeServerInfo): TreeNode => {
    const totalUsed = vs.volumes.reduce((s, v) => s + v.used, 0)
    const totalSize = vs.volumes.reduce((s, v) => s + v.size, 0)
    const usedPct = totalSize > 0 ? (totalUsed / totalSize) * 100 : 0

    const volumesNode: TreeNode = {
      key: `vs-${vs.node.id}-volumes`,
      title: (
        <span>
          <DatabaseOutlined style={{ marginRight: 4 }} />
          Volumes ({vs.volumes.length})
        </span>
      ),
      children: vs.volumes.map(v => ({
        key: `vol-${v.id}`,
        title: (
          <Tooltip title={`Collection: ${v.collection}`}>
            <span>
              <Tag color={v.status === 'available' ? 'success' : v.status === 'full' ? 'warning' : 'default'}>
                #{v.id}
              </Tag>
              <Text strong>{v.collection}</Text>
              <Text type="secondary" style={{ marginLeft: 8 }}>
                {formatBytes(v.used)} / {formatBytes(v.size)}
              </Text>
              <Text type="secondary" style={{ marginLeft: 8 }}>
                {v.file_count} files
              </Text>
            </span>
          </Tooltip>
        ),
      })),
    }

    return {
      key: `vs-${vs.node.id}`,
      title: (
        <Tooltip title={`${vs.node.address}:${vs.node.grpc_port}`}>
          <span
            onClick={() => setDrawer({ kind: 'volume', data: vs })}
            style={{ cursor: 'pointer' }}
          >
            {vs.node.status === 'online' || vs.node.status === 'healthy' ? (
              <CheckCircleFilled style={{ color: '#52c41a', marginRight: 4 }} />
            ) : (
              <WarningFilled style={{ color: '#faad14', marginRight: 4 }} />
            )}
            <Text strong>{vs.node.id}</Text>
            <Text type="secondary" style={{ marginLeft: 8 }}>
              {t('common:cpu')} {vs.node.cpu_usage.toFixed(0)}% · {vs.volumes.length} volumes
            </Text>
            {totalSize > 0 && (
              <Progress
                percent={Math.round(usedPct)}
                size="small"
                style={{ width: 120, marginLeft: 12, display: 'inline-block' }}
                strokeColor={usedPct > 90 ? '#ff4d4f' : usedPct > 70 ? '#faad14' : '#52c41a'}
              />
            )}
          </span>
        </Tooltip>
      ),
      children: [volumesNode],
    }
  }

  const buildFlowGraph = (data: TopologyData): { nodes: FlowNode[]; edges: Edge[] } => {
    const nodes: FlowNode[] = []
    const edges: Edge[] = []
    const masterY = 30
    const filerY = 260
    const volumeY = 510
    const colW = 230

    data.masters.forEach((m, i) => {
      nodes.push({
        id: `master-${m.id}`,
        type: 'default',
        position: { x: i * colW + 40, y: masterY },
        data: {
          label: (
            <div onClick={() => setDrawer({ kind: 'master', data: m })} style={{ cursor: 'pointer', minWidth: 160 }}>
              <Space direction="vertical" size={2}>
                <Tag color={m.is_leader ? 'gold' : 'blue'} icon={m.is_leader ? <CrownOutlined /> : <TeamOutlined />}>
                  {m.is_leader ? 'LEADER' : 'FOLLOWER'} · {m.id}
                </Tag>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {m.address}:{m.grpc_port}
                </Text>
                <Text style={{ fontSize: 12 }}>
                  {t('common:cpu')} {m.cpu_usage.toFixed(0)}% · {t('common:memory')} {m.mem_usage.toFixed(0)}%
                </Text>
              </Space>
            </div>
          ),
        },
        style: {
          background: m.is_leader ? '#fffbe6' : '#e6f4ff',
          border: `1px solid ${m.is_leader ? '#d4b106' : '#1677ff'}`,
          borderRadius: 8,
          padding: 8,
        },
      })
    })

    data.filers.forEach((f, i) => {
      nodes.push({
        id: `filer-${f.node_id}`,
        type: 'default',
        position: { x: i * colW + 40, y: filerY },
        data: {
          label: (
            <div onClick={() => setDrawer({ kind: 'filer', data: f })} style={{ cursor: 'pointer', minWidth: 160 }}>
              <Space direction="vertical" size={2}>
                <Tag color={f.is_healthy ? 'success' : 'error'} icon={<DashboardOutlined />}>
                  FILER · {f.node_id}
                </Tag>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {f.address}:{f.grpc_port}
                </Text>
                <Text style={{ fontSize: 12 }}>
                  {f.total_shards} shards · {f.leader_count} leader
                </Text>
              </Space>
            </div>
          ),
        },
        style: {
          background: '#f9f0ff',
          border: '1px solid #722ed1',
          borderRadius: 8,
          padding: 8,
        },
      })
    })

    data.volume_servers.forEach((vs, i) => {
      const totalUsed = vs.volumes.reduce((s, v) => s + v.used, 0)
      const totalSize = vs.volumes.reduce((s, v) => s + v.size, 0)
      const usedPct = totalSize > 0 ? Math.round((totalUsed / totalSize) * 100) : 0
      nodes.push({
        id: `vs-${vs.node.id}`,
        type: 'default',
        position: { x: i * colW + 40, y: volumeY },
        data: {
          label: (
            <div onClick={() => setDrawer({ kind: 'volume', data: vs })} style={{ cursor: 'pointer', minWidth: 160 }}>
              <Space direction="vertical" size={2}>
                <Tag color="cyan" icon={<HddOutlined />}>
                  VOLUME · {vs.node.id}
                </Tag>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {vs.node.address}:{vs.node.grpc_port}
                </Text>
                <Text style={{ fontSize: 12 }}>
                  {vs.volumes.length} vols · {t('common:cpu')} {vs.node.cpu_usage.toFixed(0)}%
                </Text>
                {totalSize > 0 && (
                  <Progress
                    percent={usedPct}
                    size="small"
                    style={{ width: 160 }}
                    strokeColor={usedPct > 90 ? '#ff4d4f' : usedPct > 70 ? '#faad14' : '#52c41a'}
                  />
                )}
              </Space>
            </div>
          ),
        },
        style: {
          background: '#e6fffb',
          border: '1px solid #13c2c2',
          borderRadius: 8,
          padding: 8,
        },
      })
    })

    data.masters.forEach(m => {
      data.filers.forEach(f => {
        edges.push({
          id: `m-${m.id}-f-${f.node_id}`,
          source: `master-${m.id}`,
          target: `filer-${f.node_id}`,
          type: 'smoothstep',
          animated: true,
          style: { stroke: '#1677ff', strokeWidth: 1.5 },
          markerEnd: { type: MarkerType.ArrowClosed, color: '#1677ff' },
        })
      })
    })

    data.filers.forEach(f => {
      data.volume_servers.forEach(vs => {
        edges.push({
          id: `f-${f.node_id}-vs-${vs.node.id}`,
          source: `filer-${f.node_id}`,
          target: `vs-${vs.node.id}`,
          type: 'smoothstep',
          animated: true,
          style: { stroke: '#13c2c2', strokeWidth: 1.2 },
          markerEnd: { type: MarkerType.ArrowClosed, color: '#13c2c2' },
        })
      })
    })

    return { nodes, edges }
  }

  const flowGraph = useMemo(() => (topology ? buildFlowGraph(topology) : { nodes: [], edges: [] }), [topology])

  const drawerTitle = useMemo(() => {
    if (!drawer) return ''
    if (drawer.kind === 'master') return `Master — ${drawer.data.id}`
    if (drawer.kind === 'filer') return `Filer — ${drawer.data.node_id}`
    return `Volume Server — ${drawer.data.node.id}`
  }, [drawer])

  return (
    <div style={{ padding: '24px' }}>
      <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            <ClusterOutlined style={{ marginRight: 8 }} />
            {t('nav:items.clusterTopology')}
          </Title>
          <Text type="secondary">
            Real-time view of Master → Filer → Volume Server → Volume hierarchy
          </Text>
        </Col>
        <Col>
          <Space>
            <Segmented<ViewMode>
              value={view}
              onChange={setView}
              options={[
                { value: 'graph', label: t('common:graphView'), icon: <ClusterOutlined /> },
                { value: 'tree', label: t('common:treeView'), icon: <InfoCircleOutlined /> },
              ]}
            />
            <Tag color="blue" icon={<ReloadOutlined />} onClick={loadTopology} style={{ cursor: 'pointer' }}>
              {t('common:refresh')}
            </Tag>
          </Space>
        </Col>
      </Row>

      {topology && (
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={`${t('common:master')} Nodes`}
                value={topology.masters.length}
                valueStyle={{ color: '#1677ff' }}
                prefix={<CloudServerOutlined />}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={`${t('common:filer')} Nodes`}
                value={topology.filers.length}
                valueStyle={{ color: '#722ed1' }}
                prefix={<CloudServerOutlined />}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={`${t('common:volume')} Servers`}
                value={topology.volume_servers.length}
                valueStyle={{ color: '#13c2c2' }}
                prefix={<HddOutlined />}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="Total Volumes"
                value={topology.volume_servers.reduce((s, vs) => s + vs.volumes.length, 0)}
                valueStyle={{ color: '#52c41a' }}
                prefix={<DatabaseOutlined />}
              />
            </Card>
          </Col>
        </Row>
      )}

      <Spin spinning={loading}>
        {view === 'tree' ? (
          <Card
            title={
              <span>
                <InfoCircleOutlined style={{ marginRight: 8 }} />
                Topology Tree
              </span>
            }
          >
            {topology && (
              <Tree
                treeData={buildTree(topology)}
                defaultExpandedKeys={['root', 'masters', 'filers', 'volumes']}
                blockNode
              />
            )}
          </Card>
        ) : (
          <Card
            title={
              <span>
                <ClusterOutlined style={{ marginRight: 8 }} />
                Layered Graph (Master → Filer → Volume Server)
              </span>
            }
            styles={{ body: { height: 780, padding: 0 } }}
          >
            {topology && (
              <ReactFlow
                nodes={flowGraph.nodes}
                edges={flowGraph.edges}
                fitView
                nodesDraggable
                nodesConnectable={false}
                proOptions={{ hideAttribution: true }}
              >
                <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
                <MiniMap pannable zoomable />
                <Controls />
              </ReactFlow>
            )}
          </Card>
        )}
      </Spin>

      <Drawer
        title={drawerTitle}
        open={!!drawer}
        onClose={() => setDrawer(null)}
        width={560}
        extra={<Button onClick={() => setDrawer(null)}>{t('common:close')}</Button>}
      >
        {drawer?.kind === 'master' && <MasterDrawer node={drawer.data} t={t} />}
        {drawer?.kind === 'filer' && <FilerDrawer node={drawer.data} t={t} />}
        {drawer?.kind === 'volume' && <VolumeDrawer vs={drawer.data} t={t} />}
      </Drawer>
    </div>
  )
}

function MasterDrawer({ node, t }: { node: NodeInfo; t: (k: string, opts?: any) => string }) {
  return (
    <Descriptions column={1} bordered size="small">
      <Descriptions.Item label="ID">{node.id}</Descriptions.Item>
      <Descriptions.Item label={t('common:role')}>
        <Tag color={node.is_leader ? 'gold' : 'blue'}>
          {node.is_leader ? t('common:leader') : t('common:follower')}
        </Tag>
      </Descriptions.Item>
      <Descriptions.Item label={t('common:address')}>
        {node.address}:{node.grpc_port} / HTTP {node.http_port}
      </Descriptions.Item>
      <Descriptions.Item label={t('common:status')}>{node.status}</Descriptions.Item>
      {node.raft_term !== undefined && (
        <Descriptions.Item label="Raft Term">{node.raft_term}</Descriptions.Item>
      )}
      <Descriptions.Item label={t('common:cpu')}>{node.cpu_usage.toFixed(1)}%</Descriptions.Item>
      <Descriptions.Item label={t('common:memory')}>{node.mem_usage.toFixed(1)}%</Descriptions.Item>
      <Descriptions.Item label={t('common:disk')}>{node.disk_usage.toFixed(1)}%</Descriptions.Item>
      <Descriptions.Item label="Uptime (s)">{node.uptime}</Descriptions.Item>
      <Descriptions.Item label="Network RX/TX">
        {formatBytes(node.network_rx)} / {formatBytes(node.network_tx)}
      </Descriptions.Item>
    </Descriptions>
  )
}

function FilerDrawer({ node, t }: { node: FilerNodeInfo; t: (k: string) => string }) {
  return (
    <Descriptions column={1} bordered size="small">
      <Descriptions.Item label="ID">{node.node_id}</Descriptions.Item>
      <Descriptions.Item label={t('common:address')}>
        {node.address}:{node.grpc_port} / HTTP {node.http_port}
      </Descriptions.Item>
      <Descriptions.Item label={t('common:status')}>
        {node.is_healthy ? (
          <Tag color="success">HEALTHY</Tag>
        ) : (
          <Tag color="error">UNHEALTHY</Tag>
        )}
      </Descriptions.Item>
      <Descriptions.Item label="Shards Total">{node.total_shards}</Descriptions.Item>
      <Descriptions.Item label="Leaders">{node.leader_count}</Descriptions.Item>
    </Descriptions>
  )
}

function VolumeDrawer({ vs, t }: { vs: VolumeServerInfo; t: (k: string) => string }) {
  const totalUsed = vs.volumes.reduce((s, v) => s + v.used, 0)
  const totalSize = vs.volumes.reduce((s, v) => s + v.size, 0)
  const usedPct = totalSize > 0 ? Math.round((totalUsed / totalSize) * 100) : 0
  return (
    <Space direction="vertical" style={{ width: '100%' }} size="large">
      <Descriptions column={1} bordered size="small">
        <Descriptions.Item label="ID">{vs.node.id}</Descriptions.Item>
        <Descriptions.Item label={t('common:address')}>
          {vs.node.address}:{vs.node.grpc_port} / HTTP {vs.node.http_port}
        </Descriptions.Item>
        <Descriptions.Item label={t('common:status')}>{vs.node.status}</Descriptions.Item>
        <Descriptions.Item label={t('common:cpu')}>{vs.node.cpu_usage.toFixed(1)}%</Descriptions.Item>
        <Descriptions.Item label={t('common:memory')}>{vs.node.mem_usage.toFixed(1)}%</Descriptions.Item>
        <Descriptions.Item label={t('common:disk')}>{vs.node.disk_usage.toFixed(1)}%</Descriptions.Item>
        <Descriptions.Item label="Volume Count">{vs.volumes.length}</Descriptions.Item>
        <Descriptions.Item label="Capacity Used / Total">
          {formatBytes(totalUsed)} / {formatBytes(totalSize)}
          <Progress
            percent={usedPct}
            size="small"
            style={{ marginTop: 4 }}
            strokeColor={usedPct > 90 ? '#ff4d4f' : usedPct > 70 ? '#faad14' : '#52c41a'}
          />
        </Descriptions.Item>
      </Descriptions>
      <Card title={`Volumes (${vs.volumes.length})`} size="small">
        {vs.volumes.length === 0 ? (
          <Text type="secondary">{t('common:noData')}</Text>
        ) : (
          <Space direction="vertical" style={{ width: '100%' }} size="small">
            {vs.volumes.map(v => {
              const p = v.size > 0 ? Math.round((v.used / v.size) * 100) : 0
              return (
                <Card key={v.id} size="small" styles={{ body: { padding: '8px 12px' } }}>
                  <Row gutter={8} align="middle">
                    <Col span={5}>
                      <Tag color={v.status === 'available' ? 'success' : v.status === 'full' ? 'warning' : 'default'}>
                        #{v.id}
                      </Tag>
                    </Col>
                    <Col span={5}>{v.collection}</Col>
                    <Col span={8}>
                      <Progress
                        percent={p}
                        size="small"
                        strokeColor={p > 90 ? '#ff4d4f' : p > 70 ? '#faad14' : '#52c41a'}
                        format={() => `${formatBytes(v.used)} / ${formatBytes(v.size)}`}
                      />
                    </Col>
                    <Col span={6}>
                      <Text type="secondary">{v.file_count} files · {v.status}</Text>
                    </Col>
                  </Row>
                </Card>
              )
            })}
          </Space>
        )}
      </Card>
    </Space>
  )
}

export default ClusterTopology
