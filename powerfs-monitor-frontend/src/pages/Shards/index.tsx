import { useState, useEffect, useCallback } from 'react'
import {
  Card, Table, Tag, Drawer, Descriptions, Spin, message, Tooltip, Typography, Space, Row, Col, Progress, Empty, Button, Select, Alert, Segmented,
} from 'antd'
import {
  DatabaseOutlined, ThunderboltOutlined, ApartmentOutlined,
  RiseOutlined, FallOutlined, NodeIndexOutlined, InfoCircleOutlined, HeartOutlined,
  CloudServerOutlined, GlobalOutlined, CheckCircleOutlined, WarningOutlined,
} from '@ant-design/icons'
import type { ShardDetail, FilerNode, ClusterShard } from '@/types'
import { getFilerNodeShards, getFilerNodes, getFilerClusterShards } from '@/services/api'
import { isNodeAlive } from '@/utils/format'
import ReactECharts from 'echarts-for-react'

const { Text, Title } = Typography

type ViewMode = 'node' | 'cluster'

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
  // ── 视图模式: 单节点 / 集群 ──
  const [viewMode, setViewMode] = useState<ViewMode>('node')

  // ── 节点列表 (10s 轮询) ──
  const [nodes, setNodes] = useState<FilerNode[]>([])
  const [nodesLoading, setNodesLoading] = useState(true)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)

  // ── 单节点 shard 数据 (15s 轮询) ──
  const [shards, setShards] = useState<ShardDetail[]>([])
  const [shardsLoading, setShardsLoading] = useState(true)
  const [selectedShard, setSelectedShard] = useState<ShardDetail | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)

  // ── 集群 shard 数据 (15s 轮询) ──
  const [clusterShards, setClusterShards] = useState<ClusterShard[]>([])
  const [clusterLoading, setClusterLoading] = useState(true)
  const [selectedClusterShard, setSelectedClusterShard] = useState<ClusterShard | null>(null)
  const [clusterDrawerOpen, setClusterDrawerOpen] = useState(false)

  const loadNodes = useCallback(async () => {
    setNodesLoading(true)
    try {
      const data = await getFilerNodes()
      setNodes(data)
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

  const loadClusterShards = useCallback(async () => {
    setClusterLoading(true)
    try {
      const data = await getFilerClusterShards()
      setClusterShards(data)
    } catch (error) {
      console.error('Failed to load cluster shards:', error)
      message.error('加载集群分片列表失败')
      setClusterShards([])
    } finally {
      setClusterLoading(false)
    }
  }, [])

  useEffect(() => {
    loadNodes()
    const timer = setInterval(loadNodes, 10000)
    return () => clearInterval(timer)
  }, [loadNodes])

  useEffect(() => {
    if (viewMode === 'node' && selectedNodeId) {
      loadShards(selectedNodeId)
      setDrawerOpen(false)
      setSelectedShard(null)
    } else if (viewMode === 'cluster') {
      loadClusterShards()
      setClusterDrawerOpen(false)
      setSelectedClusterShard(null)
    }
  }, [selectedNodeId, viewMode, loadShards, loadClusterShards])

  // 集群 shard 轮询 (15s)
  useEffect(() => {
    if (viewMode !== 'cluster') return
    const timer = setInterval(loadClusterShards, 15000)
    return () => clearInterval(timer)
  }, [viewMode, loadClusterShards])

  // 单节点 shard 轮询 (15s)
  useEffect(() => {
    if (viewMode !== 'node' || !selectedNodeId) return
    const timer = setInterval(() => loadShards(selectedNodeId), 15000)
    return () => clearInterval(timer)
  }, [viewMode, selectedNodeId, loadShards])

  const handleReload = () => {
    if (viewMode === 'cluster') loadClusterShards()
    else if (selectedNodeId) loadShards(selectedNodeId)
  }

  // ── 单节点 KPI ──
  const totalInodes = shards.reduce((sum, s) => sum + s.inode_count, 0)
  const leaderCount = shards.filter(s => s.is_leader).length
  const totalWriteQps = shards.reduce((sum, s) => sum + s.write_qps, 0)
  const totalReadQps = shards.reduce((sum, s) => sum + s.read_qps, 0)

  // ── 集群 KPI ──
  const clusterHealthyCount = clusterShards.filter(s => s.is_healthy).length
  const clusterUnhealthyCount = clusterShards.length - clusterHealthyCount
  const clusterTotalReplicas = clusterShards.reduce((sum, s) => sum + s.replicas.length, 0)
  const clusterTotalLeaders = clusterShards.reduce(
    (sum, s) => sum + s.replicas.filter(r => r.is_leader).length, 0
  )

  const selectedNode = nodes.find(n => n.node_id === selectedNodeId)
  const nodeOffline = selectedNode && !isNodeAlive(selectedNode.heartbeat_status)

  const inodePieOption = {
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    legend: { bottom: 0, type: 'scroll' },
    series: [{
      type: 'pie', radius: ['40%', '70%'], avoidLabelOverlap: false,
      itemStyle: { borderRadius: 6, borderColor: '#fff', borderWidth: 2 },
      label: { show: false, position: 'center' },
      emphasis: { label: { show: true, fontSize: 16, fontWeight: 'bold' } },
      labelLine: { show: false },
      data: shards.map(s => ({ name: `Shard ${s.shard_id}`, value: s.inode_count })),
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

  const nodeColumns = [
    { title: '分片 ID', dataIndex: 'shard_id', key: 'shard_id', width: 80, render: (id: number) => <Text strong>{id}</Text> },
    { title: '角色', dataIndex: 'is_leader', key: 'is_leader', width: 90, render: (isLeader: boolean) => isLeader ? <Tag color="gold" icon={<ThunderboltOutlined />}>Leader</Tag> : <Tag>Follower</Tag> },
    { title: 'Inode 范围', key: 'range', width: 180, render: (_: unknown, r: ShardDetail) => <Text code style={{ fontSize: 12 }}>{formatRange(r.inode_range_start, r.inode_range_end)}</Text> },
    { title: '同步状态', key: 'synced', width: 100, render: (_: unknown, r: ShardDetail) => r.commit_index === r.applied_index ? <Tag color="success">同步</Tag> : <Tag color="warning">滞后</Tag> },
    { title: 'Inode 数', dataIndex: 'inode_count', key: 'inode_count', width: 100, sorter: (a: ShardDetail, b: ShardDetail) => a.inode_count - b.inode_count, render: (c: number) => <Space><NodeIndexOutlined /><Text strong>{c}</Text></Space> },
    { title: '文件/目录', key: 'file_dir', width: 120, render: (_: unknown, r: ShardDetail) => <Space split={<Text type="secondary">/</Text>}><span><RiseOutlined /> {r.file_count}</span><span><ApartmentOutlined /> {r.dir_count}</span></Space> },
    { title: '读 QPS', dataIndex: 'read_qps', key: 'read_qps', width: 90, sorter: (a: ShardDetail, b: ShardDetail) => a.read_qps - b.read_qps, render: (q: number) => <Text style={{ color: '#52c41a' }}>{q}</Text> },
    { title: '写 QPS', dataIndex: 'write_qps', key: 'write_qps', width: 90, sorter: (a: ShardDetail, b: ShardDetail) => a.write_qps - b.write_qps, render: (q: number) => <Text style={{ color: '#1677ff' }}>{q}</Text> },
    { title: '操作', key: 'actions', width: 90, render: (_: unknown, r: ShardDetail) => <Button type="link" size="small" onClick={(e) => { e.stopPropagation(); handleNodeShardClick(r) }}>详情</Button> },
  ]

  // ── 集群 shard 列列定义 (异常 shard 高亮) ──
  const clusterColumns = [
    { title: '分片 ID', dataIndex: 'shard_id', key: 'shard_id', width: 80, render: (id: number) => <Text strong>{id}</Text> },
    { title: '健康状态', key: 'health', width: 110, render: (_: unknown, r: ClusterShard) => r.is_healthy
      ? <Tag color="success" icon={<CheckCircleOutlined />}>健康</Tag>
      : <Tooltip title={r.lag_reason}><Tag color="error" icon={<WarningOutlined />}>异常</Tag></Tooltip> },
    { title: '副本数', key: 'replicas', width: 80, render: (_: unknown, r: ClusterShard) => <Space><NodeIndexOutlined />{r.replicas.length}</Space> },
    { title: 'Inode 范围', key: 'range', width: 180, render: (_: unknown, r: ClusterShard) => <Text code style={{ fontSize: 12 }}>{formatRange(r.inode_range_start, r.inode_range_end)}</Text> },
    {
      title: 'Leader 分布', key: 'leaders', width: 180,
      render: (_: unknown, r: ClusterShard) => {
        const leaders = r.replicas.filter(rep => rep.is_leader)
        if (leaders.length === 0) return <Tag color="error">无 Leader</Tag>
        if (leaders.length > 1) return <Tooltip title="多副本同时声明 Leader, 可能脑裂"><Tag color="error">{leaders.length} Leader (脑裂?)</Tag></Tooltip>
        return <Tag color="gold">{leaders[0].node_id}</Tag>
      },
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
      title: 'Commit 滞后', key: 'commit_lag', width: 120,
      render: (_: unknown, r: ClusterShard) => {
        const commits = r.replicas.map(rep => rep.commit_index)
        const max = Math.max(...commits)
        const min = Math.min(...commits)
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
        return <Space split={<Text type="secondary">/</Text>}>
          <span style={{ color: '#52c41a' }}>{readQps}</span>
          <span style={{ color: '#1677ff' }}>{writeQps}</span>
        </Space>
      },
    },
    { title: '操作', key: 'actions', width: 90, render: (_: unknown, r: ClusterShard) => <Button type="link" size="small" onClick={(e) => { e.stopPropagation(); handleClusterShardClick(r) }}>详情</Button> },
  ]

  const handleNodeShardClick = (record: ShardDetail) => {
    setSelectedShard(record)
    setDrawerOpen(true)
  }

  const handleClusterShardClick = (record: ClusterShard) => {
    setSelectedClusterShard(record)
    setClusterDrawerOpen(true)
  }

  // ═══════════ 集群视图 ═══════════
  const clusterView = (
    <Spin spinning={clusterLoading && !clusterShards.length}>
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={12} md={6}><Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>分片总数</div><div style={{ fontSize: 28, fontWeight: 700 }}>{clusterShards.length}</div></div></Card></Col>
        <Col xs={12} md={6}><Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>健康分片</div><div style={{ fontSize: 28, fontWeight: 700, color: 'var(--pf-color-success)' }}>{clusterHealthyCount}</div></div></Card></Col>
        <Col xs={12} md={6}><Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>异常分片</div><div style={{ fontSize: 28, fontWeight: 700, color: clusterUnhealthyCount > 0 ? 'var(--pf-color-error)' : undefined }}>{clusterUnhealthyCount}</div></div></Card></Col>
        <Col xs={12} md={6}><Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>副本/Leader</div><div style={{ fontSize: 20, fontWeight: 700 }}>{clusterTotalReplicas} / {clusterTotalLeaders}</div></div></Card></Col>
      </Row>

      {clusterUnhealthyCount > 0 && (
        <Alert
          type="error" showIcon banner
          message={`检测到 ${clusterUnhealthyCount} 个异常分片`}
          description="异常分片可能存在 term 不一致或 commit_index 滞后，请检查对应 Filer 节点的 Raft 状态。"
          style={{ marginBottom: 16 }}
        />
      )}

      <Card
        title={<Space><GlobalOutlined />集群分片总览 (按 shard_id 聚合多副本)</Space>}
        size="small"
        extra={<Button icon={<DatabaseOutlined />} onClick={loadClusterShards} size="small">刷新</Button>}
      >
        {clusterShards.length > 0 ? (
          <Table
            columns={clusterColumns}
            dataSource={clusterShards}
            rowKey="shard_id"
            pagination={false}
            size="middle"
            rowClassName={(r) => !r.is_healthy ? 'pf-row-error' : ''}
            onRow={(record) => ({ onClick: () => handleClusterShardClick(record), style: { cursor: 'pointer' } })}
          />
        ) : (
          <Empty description={clusterLoading ? '加载中...' : '集群暂无分片'} />
        )}
      </Card>
    </Spin>
  )

  // ═══════════ 单节点视图 ═══════════
  const nodeView = (
    <Spin spinning={shardsLoading && !shards.length}>
      {/* 节点选择器 */}
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
              label: <Space>
                <Text>{n.node_id}</Text>
                <Text type="secondary" style={{ fontSize: 12 }}>{n.address}:{n.http_port}</Text>
                <Tag color={isNodeAlive(n.heartbeat_status) ? 'success' : 'error'} style={{ margin: 0, fontSize: 11 }}>{isNodeAlive(n.heartbeat_status) ? '在线' : '离线'}</Tag>
              </Space>,
            }))}
            notFoundContent={nodesLoading ? '加载节点中...' : '集群暂无 Filer 节点'}
          />
          {selectedNode && (
            <Tag color={isNodeAlive(selectedNode.heartbeat_status) ? 'success' : 'error'} icon={<HeartOutlined />}>
              {isNodeAlive(selectedNode.heartbeat_status) ? '心跳在线' : '心跳离线'}
            </Tag>
          )}
        </div>
      </Card>

      {!selectedNodeId ? (
        <Card><Empty description={nodesLoading ? '加载节点中...' : '请先选择一个 Filer 节点'} /></Card>
      ) : nodeOffline ? (
        <Card><Alert type="warning" showIcon message={`节点 ${selectedNodeId} 心跳离线`} description="该节点当前不可达，无法获取分片数据。" /></Card>
      ) : (
        <>
          <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
            <Col xs={12} md={6}><Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>分片总数</div><div style={{ fontSize: 28, fontWeight: 700 }}>{shards.length}</div></div></Card></Col>
            <Col xs={12} md={6}><Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>Leader 分片</div><div style={{ fontSize: 28, fontWeight: 700, color: 'var(--pf-color-success)' }}>{leaderCount}</div></div></Card></Col>
            <Col xs={12} md={6}><Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>Inode 总数</div><div style={{ fontSize: 28, fontWeight: 700 }}>{totalInodes}</div></div></Card></Col>
            <Col xs={12} md={6}><Card><div style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>读写 QPS</div><div style={{ fontSize: 20, fontWeight: 700 }}><span style={{ color: '#52c41a' }}>{totalReadQps}</span> / <span style={{ color: '#1677ff' }}>{totalWriteQps}</span></div></div></Card></Col>
          </Row>

          <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
            <Col xs={24} md={10}>
              <Card title="Inode 分布" size="small">
                {shards.length > 0 ? <ReactECharts option={inodePieOption} style={{ height: 260 }} /> : <Empty description="暂无数据" style={{ padding: 40 }} />}
              </Card>
            </Col>
            <Col xs={24} md={14}>
              <Card title="读写 QPS 性能" size="small">
                {shards.length > 0 ? <ReactECharts option={qpsBarOption} style={{ height: 260 }} /> : <Empty description="暂无数据" style={{ padding: 40 }} />}
              </Card>
            </Col>
          </Row>

          <Card title={`分片列表 (节点 ${selectedNodeId})`} size="small">
            <Table
              columns={nodeColumns}
              dataSource={shards}
              rowKey="shard_id"
              pagination={false}
              size="middle"
              onRow={(record) => ({ onClick: () => handleNodeShardClick(record), style: { cursor: 'pointer' } })}
            />
          </Card>
        </>
      )}
    </Spin>
  )

  return (
    <div>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <DatabaseOutlined style={{ fontSize: 24, color: 'var(--pf-color-primary)' }} />
          <Title level={4} style={{ margin: 0 }}>分片管理</Title>
        </Space>
        <Space>
          <Segmented
            value={viewMode}
            onChange={(v) => setViewMode(v as ViewMode)}
            options={[
              { label: <Space size={4}><CloudServerOutlined />单节点</Space>, value: 'node' },
              { label: <Space size={4}><GlobalOutlined />集群</Space>, value: 'cluster' },
            ]}
          />
          <Button icon={<DatabaseOutlined />} onClick={handleReload} size="small">刷新</Button>
        </Space>
      </div>

      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            {viewMode === 'node'
              ? '单节点视图展示选中 Filer 节点持有的分片。切换到「集群」视图查看所有分片的多副本健康状态。'
              : '集群视图按 shard_id 聚合所有 Filer 节点的副本，检测 term 不一致和 commit_index 滞后。异常分片会高亮显示。'}
          </Text>
        </div>
      </Card>

      {viewMode === 'cluster' ? clusterView : nodeView}

      {/* ── 单节点 shard 详情 Drawer ── */}
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
              <Descriptions.Item label="角色">{selectedShard.is_leader ? <Tag color="gold">Leader</Tag> : <Tag>Follower</Tag>}</Descriptions.Item>
              <Descriptions.Item label="Inode 范围">{formatRange(selectedShard.inode_range_start, selectedShard.inode_range_end)}</Descriptions.Item>
              <Descriptions.Item label="同步状态">{selectedShard.commit_index === selectedShard.applied_index ? <Tag color="success">已同步</Tag> : <Tag color="warning">滞后 {selectedShard.commit_index - selectedShard.applied_index} 条</Tag>}</Descriptions.Item>
            </Descriptions>
            <Card title="元数据统计" size="small" style={{ marginBottom: 16 }}>
              <Row gutter={16}>
                <Col span={8} style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>Inode 数</div><div style={{ fontSize: 22, fontWeight: 700 }}>{selectedShard.inode_count}</div></Col>
                <Col span={8} style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>文件数</div><div style={{ fontSize: 22, fontWeight: 700 }}>{selectedShard.file_count}</div></Col>
                <Col span={8} style={{ textAlign: 'center' }}><div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>目录数</div><div style={{ fontSize: 22, fontWeight: 700 }}>{selectedShard.dir_count}</div></Col>
              </Row>
            </Card>
            <Card title="性能指标" size="small">
              <Space direction="vertical" style={{ width: '100%' }}>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}><span><FallOutlined /> 读 QPS</span><Text strong style={{ color: '#52c41a' }}>{selectedShard.read_qps}</Text></div>
                  <Progress percent={Math.min((selectedShard.read_qps / Math.max(totalReadQps, 1)) * 100, 100)} showInfo={false} strokeColor="#52c41a" />
                </div>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}><span><RiseOutlined /> 写 QPS</span><Text strong style={{ color: '#1677ff' }}>{selectedShard.write_qps}</Text></div>
                  <Progress percent={Math.min((selectedShard.write_qps / Math.max(totalWriteQps, 1)) * 100, 100)} showInfo={false} strokeColor="#1677ff" />
                </div>
              </Space>
            </Card>
          </>
        )}
      </Drawer>

      {/* ── 集群 shard 详情 Drawer (展示多副本) ── */}
      <Drawer
        title={selectedClusterShard ? `分片 ${selectedClusterShard.shard_id} 多副本详情` : ''}
        open={clusterDrawerOpen}
        onClose={() => setClusterDrawerOpen(false)}
        width={620}
      >
        {selectedClusterShard && (
          <>
            <Descriptions bordered column={1} size="small" style={{ marginBottom: 24 }}>
              <Descriptions.Item label="分片 ID">{selectedClusterShard.shard_id}</Descriptions.Item>
              <Descriptions.Item label="Inode 范围">{formatRange(selectedClusterShard.inode_range_start, selectedClusterShard.inode_range_end)}</Descriptions.Item>
              <Descriptions.Item label="健康状态">
                {selectedClusterShard.is_healthy
                  ? <Tag color="success" icon={<CheckCircleOutlined />}>健康</Tag>
                  : <Tooltip title={selectedClusterShard.lag_reason}><Tag color="error" icon={<WarningOutlined />}>异常</Tag></Tooltip>}
              </Descriptions.Item>
              <Descriptions.Item label="副本数">{selectedClusterShard.replicas.length}</Descriptions.Item>
            </Descriptions>

            <Card title="副本分布" size="small">
              <Table
                size="small"
                rowKey="node_id"
                pagination={false}
                dataSource={selectedClusterShard.replicas}
                columns={[
                  { title: '节点', dataIndex: 'node_id', key: 'node_id', render: (id: string) => <Text strong>{id}</Text> },
                  { title: '角色', dataIndex: 'is_leader', key: 'is_leader', width: 90, render: (isLeader: boolean) => isLeader ? <Tag color="gold">Leader</Tag> : <Tag>Follower</Tag> },
                  { title: 'Term', dataIndex: 'term', key: 'term', width: 70 },
                  { title: 'Commit', dataIndex: 'commit_index', key: 'commit', width: 80 },
                  { title: 'Applied', dataIndex: 'applied_index', key: 'applied', width: 80 },
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
