import { useEffect, useState } from 'react'
import { Card, Tree, Tag, Spin, Row, Col, Typography, Statistic, Progress, Tooltip } from 'antd'
import {
  DatabaseOutlined,
  HddOutlined,
  ReloadOutlined,
  CheckCircleFilled,
  WarningFilled,
  InfoCircleOutlined,
  CloudServerOutlined,
} from '@ant-design/icons'
import type { TopologyData, VolumeServerInfo } from '@/types'
import { getTopology } from '@/services/api'
import { formatBytes } from '@/utils/format'

const { Title, Text } = Typography

type TreeNode = {
  key: string
  title: React.ReactNode
  children?: TreeNode[]
}

function ClusterTopology() {
  const [topology, setTopology] = useState<TopologyData | null>(null)
  const [loading, setLoading] = useState(false)

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
  }, [])

  const buildTree = (data: TopologyData): TreeNode[] => {
    const root: TreeNode = {
      key: 'root',
      title: (
        <span>
          <DatabaseOutlined style={{ marginRight: 8 }} />
          PowerFS Cluster
          <Tag color="blue" style={{ marginLeft: 8 }}>
            {data.masters.length} master · {data.filers.length} filer ·{' '}
            {data.volume_servers.length} volume servers
          </Tag>
        </span>
      ),
      children: [],
    }

    // Masters group
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
            <span>
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
                CPU {m.cpu_usage.toFixed(0)}% · MEM {m.mem_usage.toFixed(0)}%
              </Text>
            </span>
          </Tooltip>
        ),
      })),
    }

    // Filers group
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
            <span>
              {f.is_healthy ? (
                <Tag color="success">HEALTHY</Tag>
              ) : (
                <Tag color="error">UNHEALTHY</Tag>
              )}
              <Text strong>{f.node_id}</Text>
              <Text type="secondary" style={{ marginLeft: 8 }}>
                {f.total_shards} shards · {f.leader_count} leader
              </Text>
            </span>
          </Tooltip>
        ),
      })),
    }

    // Volume servers group
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
              <Tag
                color={v.status === 'available' ? 'success' : v.status === 'full' ? 'warning' : 'default'}
              >
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
          <span>
            {vs.node.status === 'online' || vs.node.status === 'healthy' ? (
              <CheckCircleFilled style={{ color: '#52c41a', marginRight: 4 }} />
            ) : (
              <WarningFilled style={{ color: '#faad14', marginRight: 4 }} />
            )}
            <Text strong>{vs.node.id}</Text>
            <Text type="secondary" style={{ marginLeft: 8 }}>
              CPU {vs.node.cpu_usage.toFixed(0)}% · {vs.volumes.length} volumes
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

  return (
    <div style={{ padding: '24px' }}>
      <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            <DatabaseOutlined style={{ marginRight: 8 }} />
            Cluster Topology
          </Title>
          <Text type="secondary">
            Real-time view of Master → Filer → Volume Server → Volume hierarchy
          </Text>
        </Col>
        <Col>
          <Tag color="blue" icon={<ReloadOutlined />} onClick={loadTopology} style={{ cursor: 'pointer' }}>
            Refresh
          </Tag>
        </Col>
      </Row>

      {topology && (
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="Master Nodes"
                value={topology.masters.length}
                valueStyle={{ color: '#1677ff' }}
                prefix={<CloudServerOutlined />}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="Filer Nodes"
                value={topology.filers.length}
                valueStyle={{ color: '#722ed1' }}
                prefix={<CloudServerOutlined />}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="Volume Servers"
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

      <Card
        title={
          <span>
            <InfoCircleOutlined style={{ marginRight: 8 }} />
            Topology Tree
          </span>
        }
      >
        <Spin spinning={loading}>
          {topology && (
            <Tree
              treeData={buildTree(topology)}
              defaultExpandedKeys={['root', 'masters', 'filers', 'volumes']}
              blockNode
            />
          )}
        </Spin>
      </Card>
    </div>
  )
}

export default ClusterTopology
