import { useState, useEffect, useCallback } from 'react'
import {
  Card, Table, Tag, Drawer, Descriptions, Spin, message, Tooltip, Typography, Space, Row, Col, Empty, Button, Alert,
} from 'antd'
import {
  DatabaseOutlined, ThunderboltOutlined, ApartmentOutlined,
  RiseOutlined, FallOutlined, NodeIndexOutlined, InfoCircleOutlined,
  GlobalOutlined, CheckCircleOutlined, WarningOutlined,
} from '@ant-design/icons'
import type { ClusterShard, ClusterShardReplica } from '@/types'
import { getFilerClusterShards } from '@/services/api'
import ReactECharts from 'echarts-for-react'

const { Text, Title } = Typography

function formatRange(start: number, end: number): string {
  const formatNum = (n: number) => {
    if (n >= 1e15) return '∞'
    if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`
    if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`
    return n.toString()
  }
  return `[${formatNum(start)}, ${formatNum(end)})`
}

function Shards() {
  // ── 集群 shard 数据 (15s 轮询) — 以分片为主, 聚合所有 Filer 副本 ──
  const [clusterShards, setClusterShards] = useState<ClusterShard[]>([])
  const [loading, setLoading] = useState(true)
  const [selectedShard, setSelectedShard] = useState<ClusterShard | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)

  const loadShards = useCallback(async () => {
    setLoading(true)
    try {
      const data = await getFilerClusterShards()
      setClusterShards(data)
    } catch (error) {
      console.error('Failed to load cluster shards:', error)
      message.error('加载集群分片列表失败')
      setClusterShards([])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadShards()
    const timer = setInterval(loadShards, 15000)
    return () => clearInterval(timer)
  }, [loadShards])

  // ── KPI ──
  const totalShards = clusterShards.length
  const healthyCount = clusterShards.filter(s => s.is_healthy).length
  const unhealthyCount = totalShards - healthyCount
  const totalReplicas = clusterShards.reduce((sum, s) => sum + s.replicas.length, 0)
  const totalLeaders = clusterShards.reduce(
    (sum, s) => sum + s.replicas.filter(r => r.is_leader).length, 0
  )

  const handleShardClick = (record: ClusterShard) => {
    setSelectedShard(record)
    setDrawerOpen(true)
  }

  // ── 主表列定义: 以分片为主, 展示该分片的所有 Filer 副本 + Leader ──
  const columns = [
    {
      title: '分片 ID', dataIndex: 'shard_id', key: 'shard_id', width: 90,
      render: (id: number) => <Text strong style={{ fontSize: 15 }}>{id}</Text>,
    },
    {
      title: 'Leader', key: 'leader', width: 130,
      render: (_: unknown, r: ClusterShard) => {
        const leaders = r.replicas.filter(rep => rep.is_leader)
        if (leaders.length === 0) return <Tag color="error" icon={<WarningOutlined />}>无 Leader</Tag>
        if (leaders.length > 1) {
          return (
            <Tooltip title={`多副本同时声明 Leader, 可能脑裂: ${leaders.map(l => l.node_id).join(', ')}`}>
              <Tag color="error">{leaders.length} Leader (脑裂?)</Tag>
            </Tooltip>
          )
        }
        return <Tag color="gold" icon={<ThunderboltOutlined />}>{leaders[0].node_id}</Tag>
      },
    },
    {
      title: '副本节点 (Filer)', key: 'replicas', width: 280,
      render: (_: unknown, r: ClusterShard) => {
        if (r.replicas.length === 0) return <Text type="secondary">无副本</Text>
        // 按 node_id 排序, Leader 排在第一位
        const sorted = [...r.replicas].sort((a, b) => {
          if (a.is_leader !== b.is_leader) return a.is_leader ? -1 : 1
          return a.node_id.localeCompare(b.node_id)
        })
        return (
          <Space size={[4, 4]} wrap>
            {sorted.map(rep => (
              <Tag
                key={rep.node_id}
                color={rep.is_leader ? 'gold' : 'default'}
                icon={rep.is_leader ? <ThunderboltOutlined /> : undefined}
                style={{ margin: 0 }}
              >
                {rep.node_id}
              </Tag>
            ))}
          </Space>
        )
      },
    },
    {
      title: '副本数', key: 'replica_count', width: 80,
      sorter: (a: ClusterShard, b: ClusterShard) => a.replicas.length - b.replicas.length,
      render: (_: unknown, r: ClusterShard) => <Space><NodeIndexOutlined /><Text strong>{r.replicas.length}</Text></Space>,
    },
    {
      title: '健康状态', key: 'health', width: 110,
      render: (_: unknown, r: ClusterShard) => r.is_healthy
        ? <Tag color="success" icon={<CheckCircleOutlined />}>健康</Tag>
        : <Tooltip title={r.lag_reason}><Tag color="error" icon={<WarningOutlined />}>异常</Tag></Tooltip>,
    },
    {
      title: 'Inode 范围', key: 'range', width: 170,
      render: (_: unknown, r: ClusterShard) => <Text code style={{ fontSize: 12 }}>{formatRange(r.inode_range_start, r.inode_range_end)}</Text>,
    },
    {
      title: 'Term 一致性', key: 'term', width: 110,
      render: (_: unknown, r: ClusterShard) => {
        const terms = [...new Set(r.replicas.map(rep => rep.term))]
        return terms.length === 1
          ? <Tag color="success">一致 (T{terms[0]})</Tag>
          : <Tooltip title={`不一致: ${terms.join(', ')}`}><Tag color="error">不一致</Tag></Tooltip>
      },
    },
    {
      title: 'Commit 同步', key: 'commit_lag', width: 120,
      render: (_: unknown, r: ClusterShard) => {
        const commits = r.replicas.map(rep => rep.commit_index)
        const max = commits.length ? Math.max(...commits) : 0
        const min = commits.length ? Math.min(...commits) : 0
        const lag = max - min
        return lag === 0
          ? <Tag color="success">已同步</Tag>
          : <Tag color={lag < 100 ? 'warning' : 'error'}>滞后 {lag}</Tag>
      },
    },
    {
      title: '总读写 QPS', key: 'qps', width: 130,
      render: (_: unknown, r: ClusterShard) => {
        const readQps = r.replicas.reduce((s, rep) => s + rep.read_qps, 0)
        const writeQps = r.replicas.reduce((s, rep) => s + rep.write_qps, 0)
        return (
          <Space split={<Text type="secondary">/</Text>}>
            <span style={{ color: '#52c41a' }}><FallOutlined /> {readQps}</span>
            <span style={{ color: '#1677ff' }}><RiseOutlined /> {writeQps}</span>
          </Space>
        )
      },
    },
    {
      title: '操作', key: 'actions', width: 90,
      render: (_: unknown, r: ClusterShard) => (
        <Button type="link" size="small" onClick={(e) => { e.stopPropagation(); handleShardClick(r) }}>详情</Button>
      ),
    },
  ]

  // ── 展开行: 该分片各副本的 term/commit/applied/inode/qps 明细 ──
  const expandedRowRender = (record: ClusterShard) => {
    const sorted = [...record.replicas].sort((a, b) => {
      if (a.is_leader !== b.is_leader) return a.is_leader ? -1 : 1
      return a.node_id.localeCompare(b.node_id)
    })
    return (
      <Table<ClusterShardReplica>
        size="small"
        rowKey="node_id"
        pagination={false}
        dataSource={sorted}
        columns={[
          { title: 'Filer 节点', dataIndex: 'node_id', key: 'node_id', width: 140, render: (id: string, r: ClusterShardReplica) => (
            <Space>
              <Text strong>{id}</Text>
              {r.is_leader && <Tag color="gold" icon={<ThunderboltOutlined />} style={{ margin: 0 }}>Leader</Tag>}
            </Space>
          ) },
          { title: 'Term', dataIndex: 'term', key: 'term', width: 80 },
          { title: 'Commit', dataIndex: 'commit_index', key: 'commit', width: 90 },
          { title: 'Applied', dataIndex: 'applied_index', key: 'applied', width: 90,
            render: (applied: number, r: ClusterShardReplica) =>
              applied === r.commit_index
                ? <Text>{applied}</Text>
                : <Tooltip title={`落后 ${r.commit_index - applied} 条`}><Tag color="warning">{applied}</Tag></Tooltip>,
          },
          { title: 'Inode 数', dataIndex: 'inode_count', key: 'inode_count', width: 110, render: (c: number) => <Space><NodeIndexOutlined />{c}</Space> },
          { title: '读 QPS', dataIndex: 'read_qps', key: 'read_qps', width: 90, render: (q: number) => <Text style={{ color: '#52c41a' }}>{q}</Text> },
          { title: '写 QPS', dataIndex: 'write_qps', key: 'write_qps', width: 90, render: (q: number) => <Text style={{ color: '#1677ff' }}>{q}</Text> },
        ]}
      />
    )
  }

  // ── Leader 分布饼图 (按 Filer 节点统计每个节点持有多少 Leader) ──
  const leaderDistribution = clusterShards.reduce<Record<string, number>>((acc, s) => {
    const leader = s.replicas.find(r => r.is_leader)
    if (leader) {
      acc[leader.node_id] = (acc[leader.node_id] ?? 0) + 1
    } else {
      acc['(无 Leader)'] = (acc['(无 Leader)'] ?? 0) + 1
    }
    return acc
  }, {})
  const leaderPieOption = {
    tooltip: { trigger: 'item', formatter: '{b}: {c} 个 Leader ({d}%)' },
    legend: { bottom: 0, type: 'scroll' },
    series: [{
      type: 'pie', radius: ['40%', '70%'], avoidLabelOverlap: false,
      itemStyle: { borderRadius: 6, borderColor: '#fff', borderWidth: 2 },
      label: { show: false, position: 'center' },
      emphasis: { label: { show: true, fontSize: 14, fontWeight: 'bold' } },
      labelLine: { show: false },
      data: Object.entries(leaderDistribution).map(([name, value]) => ({ name, value })),
    }],
  }

  return (
    <div>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <DatabaseOutlined style={{ fontSize: 24, color: 'var(--pf-color-primary)' }} />
          <Title level={4} style={{ margin: 0 }}>分片管理</Title>
        </Space>
        <Button icon={<DatabaseOutlined />} onClick={loadShards} size="small">刷新</Button>
      </div>

      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            以分片为主维度展示：每个分片列出所有持有副本的 Filer 节点及 Leader。展开行可查看各副本的 Term / Commit / Applied / QPS 明细。异常分片（Term 不一致或 Commit 滞后）会高亮显示。
          </Text>
        </div>
      </Card>

      <Spin spinning={loading && !clusterShards.length}>
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col xs={12} md={6}>
            <Card>
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>分片总数</div>
                <div style={{ fontSize: 28, fontWeight: 700 }}>{totalShards}</div>
              </div>
            </Card>
          </Col>
          <Col xs={12} md={6}>
            <Card>
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>健康分片</div>
                <div style={{ fontSize: 28, fontWeight: 700, color: 'var(--pf-color-success)' }}>{healthyCount}</div>
              </div>
            </Card>
          </Col>
          <Col xs={12} md={6}>
            <Card>
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>异常分片</div>
                <div style={{ fontSize: 28, fontWeight: 700, color: unhealthyCount > 0 ? 'var(--pf-color-error)' : undefined }}>{unhealthyCount}</div>
              </div>
            </Card>
          </Col>
          <Col xs={12} md={6}>
            <Card>
              <div style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>副本 / Leader 总数</div>
                <div style={{ fontSize: 20, fontWeight: 700 }}>{totalReplicas} / {totalLeaders}</div>
              </div>
            </Card>
          </Col>
        </Row>

        {unhealthyCount > 0 && (
          <Alert
            type="error" showIcon banner
            message={`检测到 ${unhealthyCount} 个异常分片`}
            description="异常分片可能存在 Term 不一致或 Commit_index 滞后，请检查对应 Filer 节点的 Raft 状态。"
            style={{ marginBottom: 16 }}
          />
        )}

        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col xs={24} md={8}>
            <Card title={<Space><ApartmentOutlined />Leader 分布 (按 Filer)</Space>} size="small">
              {totalShards > 0
                ? <ReactECharts option={leaderPieOption} style={{ height: 240 }} />
                : <Empty description="暂无数据" style={{ padding: 30 }} />}
            </Card>
          </Col>
          <Col xs={24} md={16}>
            <Card
              title={<Space><GlobalOutlined />分片总览 (每行一个分片，列出副本 Filer 与 Leader)</Space>}
              size="small"
              extra={<Text type="secondary" style={{ fontSize: 12 }}>点击行展开副本明细</Text>}
            >
              {clusterShards.length > 0 ? (
                <Table<ClusterShard>
                  columns={columns}
                  dataSource={clusterShards}
                  rowKey="shard_id"
                  pagination={false}
                  size="middle"
                  expandable={{
                    expandedRowRender,
                    rowExpandable: (r) => r.replicas.length > 0,
                  }}
                  rowClassName={(r) => !r.is_healthy ? 'pf-row-error' : ''}
                  onRow={(record) => ({ onClick: () => handleShardClick(record), style: { cursor: 'pointer' } })}
                />
              ) : (
                <Empty description={loading ? '加载中...' : '集群暂无分片'} />
              )}
            </Card>
          </Col>
        </Row>
      </Spin>

      {/* ── 分片详情 Drawer (展示多副本) ── */}
      <Drawer
        title={selectedShard ? `分片 ${selectedShard.shard_id} 详情` : ''}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={620}
      >
        {selectedShard && (
          <>
            <Descriptions bordered column={1} size="small" style={{ marginBottom: 24 }}>
              <Descriptions.Item label="分片 ID">{selectedShard.shard_id}</Descriptions.Item>
              <Descriptions.Item label="Inode 范围">{formatRange(selectedShard.inode_range_start, selectedShard.inode_range_end)}</Descriptions.Item>
              <Descriptions.Item label="Leader">
                {(() => {
                  const leaders = selectedShard.replicas.filter(r => r.is_leader)
                  if (leaders.length === 0) return <Tag color="error" icon={<WarningOutlined />}>无 Leader</Tag>
                  if (leaders.length > 1) return <Tag color="error">{leaders.length} Leader (脑裂?)</Tag>
                  return <Tag color="gold" icon={<ThunderboltOutlined />}>{leaders[0].node_id}</Tag>
                })()}
              </Descriptions.Item>
              <Descriptions.Item label="副本数">{selectedShard.replicas.length}</Descriptions.Item>
              <Descriptions.Item label="健康状态">
                {selectedShard.is_healthy
                  ? <Tag color="success" icon={<CheckCircleOutlined />}>健康</Tag>
                  : <Tooltip title={selectedShard.lag_reason}><Tag color="error" icon={<WarningOutlined />}>异常</Tag></Tooltip>}
              </Descriptions.Item>
            </Descriptions>

            <Card title="副本分布 (该分片的所有 Filer)" size="small">
              <Table
                size="small"
                rowKey="node_id"
                pagination={false}
                dataSource={[...selectedShard.replicas].sort((a, b) => {
                  if (a.is_leader !== b.is_leader) return a.is_leader ? -1 : 1
                  return a.node_id.localeCompare(b.node_id)
                })}
                columns={[
                  { title: 'Filer 节点', dataIndex: 'node_id', key: 'node_id', render: (id: string) => <Text strong>{id}</Text> },
                  { title: '角色', dataIndex: 'is_leader', key: 'is_leader', width: 90, render: (isLeader: boolean) => isLeader ? <Tag color="gold">Leader</Tag> : <Tag>Follower</Tag> },
                  { title: 'Term', dataIndex: 'term', key: 'term', width: 70 },
                  { title: 'Commit', dataIndex: 'commit_index', key: 'commit', width: 80 },
                  { title: 'Applied', dataIndex: 'applied_index', key: 'applied', width: 80 },
                  { title: 'Inode 数', dataIndex: 'inode_count', key: 'inode_count', width: 90 },
                  { title: '读 QPS', dataIndex: 'read_qps', key: 'read', width: 80, render: (q: number) => <Text style={{ color: '#52c41a' }}>{q}</Text> },
                  { title: '写 QPS', dataIndex: 'write_qps', key: 'write', width: 80, render: (q: number) => <Text style={{ color: '#1677ff' }}>{q}</Text> },
                ]}
              />
            </Card>
          </>
        )}
      </Drawer>
    </div>
  )
}

export default Shards
