import { useState, useEffect, useCallback } from 'react'
import {
  Card, Table, Tag, Drawer, Descriptions, Spin, message, Tooltip, Typography, Space, Row, Col, Progress, Empty, Button, Select, Alert,
} from 'antd'
import {
  DatabaseOutlined, ThunderboltOutlined, ReloadOutlined, ApartmentOutlined,
  RiseOutlined, FallOutlined, NodeIndexOutlined, InfoCircleOutlined, HeartOutlined, CloudServerOutlined,
} from '@ant-design/icons'
import type { ShardDetail, FilerNode } from '@/types'
import { getFilerNodeShards, getFilerNodes } from '@/services/api'
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
  // ── 节点列表 (10s 轮询, 见 docs/filer-redesign-plan.md 决策 3) ──
  const [nodes, setNodes] = useState<FilerNode[]>([])
  const [nodesLoading, setNodesLoading] = useState(true)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)

  // ── shard 数据 (15s 轮询, 见 docs/filer-redesign-plan.md 决策 3) ──
  const [shards, setShards] = useState<ShardDetail[]>([])
  const [shardsLoading, setShardsLoading] = useState(true)
  const [selectedShard, setSelectedShard] = useState<ShardDetail | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)

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

  const loadShards = useCallback(async (nodeId: string) => {
    setShardsLoading(true)
    try {
      const data = await getFilerNodeShards(nodeId)
      setShards(data)
    } catch (error) {
      console.error('Failed to load shards:', error)
      message.error(`加载节点 ${nodeId} 的分片列表失败`)
      setShards([])
    } finally {
      setShardsLoading(false)
    }
  }, [])

  useEffect(() => {
    loadNodes()
    const timer = setInterval(loadNodes, 10000)
    return () => clearInterval(timer)
  }, [loadNodes])

  // 选中节点变化时加载 shard (并重置 Drawer)
  useEffect(() => {
    if (selectedNodeId) {
      loadShards(selectedNodeId)
      setDrawerOpen(false)
      setSelectedShard(null)
    } else {
      setShards([])
    }
  }, [selectedNodeId, loadShards])

  const handleReload = () => {
    if (selectedNodeId) loadShards(selectedNodeId)
    else loadNodes()
  }

  const totalInodes = shards.reduce((sum, s) => sum + s.inode_count, 0)
  const leaderCount = shards.filter(s => s.is_leader).length
  const totalWriteQps = shards.reduce((sum, s) => sum + s.write_qps, 0)
  const totalReadQps = shards.reduce((sum, s) => sum + s.read_qps, 0)

  const selectedNode = nodes.find(n => n.node_id === selectedNodeId)
  const nodeOffline = selectedNode && selectedNode.heartbeat_status !== 'online'

  const inodePieOption = {
    tooltip: {
      trigger: 'item',
      formatter: totalInodes > 0
        ? '{b}: {c} inodes ({d}%)'
        : '{b}: 容量 {c} ({d}%)',
    },
    legend: { bottom: 0, type: 'scroll' },
    series: [{
      type: 'pie',
      radius: ['40%', '70%'],
      avoidLabelOverlap: false,
      itemStyle: { borderRadius: 6, borderColor: '#fff', borderWidth: 2 },
      label: { show: false, position: 'center' },
      emphasis: { label: { show: true, fontSize: 16, fontWeight: 'bold' } },
      labelLine: { show: false },
      data: shards.map(s => ({
        name: `Shard ${s.shard_id}`,
        value: totalInodes > 0
          ? s.inode_count
          : Math.min(s.inode_range_end - s.inode_range_start, Number.MAX_SAFE_INTEGER),
      })),
    }],
  }

  const qpsBarOption = {
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { bottom: 0, data: ['读 QPS', '写 QPS'] },
    grid: { left: '3%', right: '4%', bottom: '15%', top: '5%', containLabel: true },
    xAxis: { type: 'category', data: shards.map(s => `Shard ${s.shard_id}`) },
    yAxis: { type: 'value', min: 0, minInterval: 1 },
    series: [
      { name: '读 QPS', type: 'bar', data: shards.map(s => s.read_qps), itemStyle: { color: '#52c41a' } },
      { name: '写 QPS', type: 'bar', data: shards.map(s => s.write_qps), itemStyle: { color: '#1677ff' } },
    ],
  }

  const columns = [
    {
      title: '分片 ID',
      dataIndex: 'shard_id',
      key: 'shard_id',
      width: 80,
      render: (id: number) => <Text strong>{id}</Text>,
    },
    {
      title: '角色',
      dataIndex: 'is_leader',
      key: 'is_leader',
      width: 90,
      render: (isLeader: boolean) =>
        isLeader ? <Tag color="gold" icon={<ThunderboltOutlined />}>Leader</Tag> : <Tag>Follower</Tag>,
    },
    {
      title: 'Inode 范围',
      key: 'range',
      width: 180,
      render: (_: unknown, record: ShardDetail) => (
        <Tooltip title={`起始: ${record.inode_range_start}  结束: ${record.inode_range_end}`}>
          <Text code style={{ fontSize: 12 }}>{formatRange(record.inode_range_start, record.inode_range_end)}</Text>
        </Tooltip>
      ),
    },
    {
      title: '同步状态',
      key: 'synced',
      width: 100,
      render: (_: unknown, record: ShardDetail) => {
        const synced = record.commit_index === record.applied_index
        return synced
          ? <Tag color="success" style={{ margin: 0 }}>同步</Tag>
          : <Tag color="warning" style={{ margin: 0 }}>滞后</Tag>
      },
    },
    {
      title: 'Inode 数',
      dataIndex: 'inode_count',
      key: 'inode_count',
      width: 100,
      sorter: (a: ShardDetail, b: ShardDetail) => a.inode_count - b.inode_count,
      render: (count: number) => (
        <Space><NodeIndexOutlined /><Text strong>{count}</Text></Space>
      ),
    },
    {
      title: '文件/目录',
      key: 'file_dir',
      width: 120,
      render: (_: unknown, record: ShardDetail) => (
        <Space split={<Text type="secondary">/</Text>}>
          <span><RiseOutlined /> {record.file_count}</span>
          <span><ApartmentOutlined /> {record.dir_count}</span>
        </Space>
      ),
    },
    {
      title: '读 QPS',
      dataIndex: 'read_qps',
      key: 'read_qps',
      width: 90,
      sorter: (a: ShardDetail, b: ShardDetail) => a.read_qps - b.read_qps,
      render: (qps: number) => <Text style={{ color: '#52c41a' }}>{qps}</Text>,
    },
    {
      title: '写 QPS',
      dataIndex: 'write_qps',
      key: 'write_qps',
      width: 90,
      sorter: (a: ShardDetail, b: ShardDetail) => a.write_qps - b.write_qps,
      render: (qps: number) => <Text style={{ color: '#1677ff' }}>{qps}</Text>,
    },
    {
      title: '操作',
      key: 'actions',
      width: 90,
      render: (_: unknown, record: ShardDetail) => (
        <Button type="link" size="small" onClick={(e) => { e.stopPropagation(); handleRowClick(record) }}>
          详情
        </Button>
      ),
    },
  ]

  const handleRowClick = (record: ShardDetail) => {
    setSelectedShard(record)
    setDrawerOpen(true)
  }

  return (
    <Spin spinning={shardsLoading && !shards.length}>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <DatabaseOutlined style={{ fontSize: 24, color: 'var(--pf-color-primary)' }} />
          <Title level={4} style={{ margin: 0 }}>分片管理</Title>
        </Space>
        <Tooltip title="刷新">
          <Button icon={<ReloadOutlined />} onClick={handleReload} size="small">刷新</Button>
        </Tooltip>
      </div>

      {/* ── 节点选择器 ── 按节点维度查看 shard 分布 (Phase B) ── */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <CloudServerOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary">Filer 节点:</Text>
          <Select
            style={{ width: 320 }}
            placeholder="选择 Filer 节点查看其分片分布"
            value={selectedNodeId ?? undefined}
            onChange={setSelectedNodeId}
            loading={nodesLoading}
            options={nodes.map(n => ({
              value: n.node_id,
              label: (
                <Space>
                  <Text>{n.node_id}</Text>
                  <Text type="secondary" style={{ fontSize: 12 }}>{n.address}:{n.http_port}</Text>
                  <Tag color={n.heartbeat_status === 'online' ? 'success' : 'error'} style={{ margin: 0, fontSize: 11 }}>
                    {n.heartbeat_status === 'online' ? '在线' : '离线'}
                  </Tag>
                </Space>
              ),
            }))}
            notFoundContent={nodesLoading ? '加载节点中...' : '集群暂无 Filer 节点'}
          />
          {selectedNode && (
            <Space size={4}>
              <Tag color={selectedNode.heartbeat_status === 'online' ? 'success' : 'error'} icon={<HeartOutlined />}>
                {selectedNode.heartbeat_status === 'online' ? '心跳在线' : '心跳离线'}
              </Tag>
              <Text type="secondary" style={{ fontSize: 12 }}>
                Leader {selectedNode.leader_count} / Shard {selectedNode.total_shards}
              </Text>
            </Space>
          )}
        </div>
      </Card>

      {!selectedNodeId ? (
        <Card>
          <Empty description={nodesLoading ? '加载节点中...' : '请先选择一个 Filer 节点'} />
        </Card>
      ) : nodeOffline ? (
        <Card>
          <Alert
            type="warning"
            showIcon
            message={`节点 ${selectedNodeId} 心跳离线`}
            description="该节点当前不可达，无法获取分片数据。请检查节点状态或选择其他在线节点。"
          />
        </Card>
      ) : (
        <>
          <Card size="small" style={{ marginBottom: 24 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
              <Text type="secondary" style={{ fontSize: 13 }}>
                分片是 PowerFS 的元数据存储单元，每个分片负责管理一定范围的 Inode。当前展示节点{' '}
                <Text strong>{selectedNodeId}</Text> 持有的分片。
                系统会自动将元数据分配到不同分片中，并通过{' '}
                <a href="/shard-balancing" style={{ color: 'var(--pf-color-primary)' }}>自动均衡器</a> 平衡各节点负载。
              </Text>
            </div>
          </Card>

          <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
            <Col xs={12} md={6}>
              <Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>分片总数</div><div style={{ fontSize: 28, fontWeight: 700 }}>{shards.length}</div></div></Card>
            </Col>
            <Col xs={12} md={6}>
              <Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>Leader 分片</div><div style={{ fontSize: 28, fontWeight: 700, color: 'var(--pf-color-success)' }}>{leaderCount}</div></div></Card>
            </Col>
            <Col xs={12} md={6}>
              <Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>Inode 总数</div><div style={{ fontSize: 28, fontWeight: 700 }}>{totalInodes}</div></div></Card>
            </Col>
            <Col xs={12} md={6}>
              <Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>读写 QPS</div><div style={{ fontSize: 20, fontWeight: 700 }}><span style={{ color: '#52c41a' }}>{totalReadQps}</span> / <span style={{ color: '#1677ff' }}>{totalWriteQps}</span></div></div></Card>
            </Col>
          </Row>

          <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
            <Col xs={24} md={10}>
              <Card title={totalInodes > 0 ? 'Inode 分布' : '分片容量分布'} size="small">
                {shards.length > 0 ? (
                  <ReactECharts option={inodePieOption} style={{ height: 260 }} />
                ) : (
                  <Empty description="暂无数据" style={{ padding: 40 }} />
                )}
              </Card>
            </Col>
            <Col xs={24} md={14}>
              <Card title="读写 QPS 性能" size="small">
                {shards.length > 0 ? (
                  <ReactECharts option={qpsBarOption} style={{ height: 260 }} />
                ) : (
                  <Empty description="暂无数据" style={{ padding: 40 }} />
                )}
              </Card>
            </Col>
          </Row>

          <Card title={`分片列表 (节点 ${selectedNodeId})`} size="small">
            <Table
              columns={columns}
              dataSource={shards}
              rowKey="shard_id"
              pagination={false}
              size="middle"
              onRow={(record) => ({ onClick: () => handleRowClick(record), style: { cursor: 'pointer' } })}
            />
          </Card>
        </>
      )}

      <Card title="常见问题" size="small" style={{ marginTop: 24 }}>
        <Descriptions column={1} size="small">
          <Descriptions.Item label="什么是分片？">
            分片是元数据的存储单元。PowerFS 将文件系统的 Inode 按范围划分到不同分片中，每个分片由一组节点管理。
          </Descriptions.Item>
          <Descriptions.Item label="什么是 Leader？">
            每个分片有一个 Leader 节点负责处理写入请求，其他节点作为 Follower 同步数据。Leader 负责决策，Follower 提供冗余。
          </Descriptions.Item>
          <Descriptions.Item label="为什么需要多个分片？">
            多个分片可以分散元数据负载，提高并发处理能力。每个分片独立处理自己范围内的元数据操作。
          </Descriptions.Item>
          <Descriptions.Item label="分片如何均衡？">
            系统内置的均衡器会自动检测各节点的负载，将过载节点的 Leader 迁移到负载较低的节点，保持集群平衡。
          </Descriptions.Item>
          <Descriptions.Item label="为什么按节点查看？">
            多 Filer 集群中，每个节点持有部分分片的 Leader。按节点查看可以定位哪个节点承载了哪些分片，便于排查负载不均问题。
          </Descriptions.Item>
        </Descriptions>
      </Card>

      <Drawer
        title={selectedShard ? `分片 ${selectedShard.shard_id} 详情` : ''}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={520}
      >
        {selectedShard && (
          <>
            <Descriptions bordered column={1} size="small" style={{ marginBottom: 24 }}>
              <Descriptions.Item label="所属节点">{selectedNodeId}</Descriptions.Item>
              <Descriptions.Item label="分片 ID">{selectedShard.shard_id}</Descriptions.Item>
              <Descriptions.Item label="角色">
                {selectedShard.is_leader ? <Tag color="gold">Leader</Tag> : <Tag>Follower</Tag>}
              </Descriptions.Item>
              <Descriptions.Item label="Inode 范围">{formatRange(selectedShard.inode_range_start, selectedShard.inode_range_end)}</Descriptions.Item>
              <Descriptions.Item label="同步状态">
                {selectedShard.commit_index === selectedShard.applied_index
                  ? <Tag color="success">已同步</Tag>
                  : <Tag color="warning">滞后 {selectedShard.commit_index - selectedShard.applied_index} 条</Tag>}
              </Descriptions.Item>
            </Descriptions>

            <Card title="元数据统计" size="small" style={{ marginBottom: 16 }}>
              <Row gutter={16}>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>Inode 数</div>
                  <div style={{ fontSize: 22, fontWeight: 700 }}>{selectedShard.inode_count}</div>
                </Col>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>文件数</div>
                  <div style={{ fontSize: 22, fontWeight: 700 }}>{selectedShard.file_count}</div>
                </Col>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>目录数</div>
                  <div style={{ fontSize: 22, fontWeight: 700 }}>{selectedShard.dir_count}</div>
                </Col>
              </Row>
            </Card>

            <Card title="性能指标" size="small" style={{ marginBottom: 16 }}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span><FallOutlined /> 读 QPS</span>
                    <Text strong style={{ color: '#52c41a' }}>{selectedShard.read_qps}</Text>
                  </div>
                  <Progress percent={Math.min((selectedShard.read_qps / Math.max(totalReadQps, 1)) * 100, 100)} showInfo={false} strokeColor="#52c41a" />
                </div>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span><RiseOutlined /> 写 QPS</span>
                    <Text strong style={{ color: '#1677ff' }}>{selectedShard.write_qps}</Text>
                  </div>
                  <Progress percent={Math.min((selectedShard.write_qps / Math.max(totalWriteQps, 1)) * 100, 100)} showInfo={false} strokeColor="#1677ff" />
                </div>
              </Space>
            </Card>

            <Card title="路由映射" size="small">
              <Descriptions column={1} size="small">
                <Descriptions.Item label="路由策略">按 Inode 范围分片</Descriptions.Item>
                <Descriptions.Item label="本分片范围">{formatRange(selectedShard.inode_range_start, selectedShard.inode_range_end)}</Descriptions.Item>
                <Descriptions.Item label="说明">分配在此范围内的文件和目录元数据将存储在本分片</Descriptions.Item>
              </Descriptions>
            </Card>
          </>
        )}
      </Drawer>
    </Spin>
  )
}

export default Shards
