import { useState, useEffect, useCallback } from 'react'
import {
  Card, Spin, Typography, Space, Row, Col, Button, Tag, Progress, Slider,
  Divider, Descriptions, Empty, message, Select, Tooltip, Alert, Popconfirm, Modal, List, Statistic,
} from 'antd'
import {
  DatabaseOutlined, PlayCircleOutlined, StopOutlined,
  InfoCircleOutlined, CheckCircleOutlined, AlertOutlined, ThunderboltOutlined,
  CloudServerOutlined, HeartOutlined, GlobalOutlined,
} from '@ant-design/icons'
import type { FilerNode, BatchResult } from '@/types'
import {
  getFilerNodes, getFilerNodeBalancerStatus, startFilerNodeBalancer,
  stopFilerNodeBalancer, triggerFilerNodeBalancer, getFilerNodeBalancerConfig,
  updateFilerNodeBalancerConfig,
  startAllBalancers, stopAllBalancers, triggerAllBalancers,
  type SchedulerStatus, type SchedulerConfig,
} from '@/services/api'
import { isNodeAlive } from '@/utils/format'

const { Text, Title } = Typography

function ShardBalancing() {
  // ── 节点列表 (10s 轮询) ──
  const [nodes, setNodes] = useState<FilerNode[]>([])
  const [nodesLoading, setNodesLoading] = useState(true)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)

  // ── 选中节点的 balancer 状态 (5s 轮询, 见 docs/filer-redesign-plan.md 决策 3) ──
  const [status, setStatus] = useState<SchedulerStatus | null>(null)
  const [statusLoading, setStatusLoading] = useState(true)
  const [config, setConfig] = useState<SchedulerConfig | null>(null)
  const [configLoading, setConfigLoading] = useState(false)
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({})

  // ── 批量操作 (Phase C: start/stop/trigger all) ──
  const [batchLoading, setBatchLoading] = useState<Record<string, boolean>>({})
  const [batchResult, setBatchResult] = useState<BatchResult | null>(null)
  const [batchModalOpen, setBatchModalOpen] = useState(false)
  const [batchAction, setBatchAction] = useState('')
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

  const loadStatus = useCallback(async (nodeId: string) => {
    setStatusLoading(true)
    try {
      const data = await getFilerNodeBalancerStatus(nodeId)
      setStatus(data)
    } catch (error) {
      console.error('Failed to load balancer status:', error)
      setStatus(null)
    } finally {
      setStatusLoading(false)
    }
  }, [])

  const loadConfig = useCallback(async (nodeId: string) => {
    try {
      const data = await getFilerNodeBalancerConfig(nodeId)
      setConfig(data)
    } catch (error) {
      console.error('Failed to load balancer config:', error)
      setConfig(null)
    }
  }, [])

  useEffect(() => {
    loadNodes()
    const timer = setInterval(loadNodes, 10000)
    return () => clearInterval(timer)
  }, [loadNodes])

  // 选中节点变化时加载状态 + 配置
  useEffect(() => {
    if (selectedNodeId) {
      loadStatus(selectedNodeId)
      loadConfig(selectedNodeId)
    } else {
      setStatus(null)
      setConfig(null)
    }
  }, [selectedNodeId, loadStatus, loadConfig])

  // balancer 状态轮询 (5s) — 只在选中节点时启用
  useEffect(() => {
    if (!selectedNodeId) return
    const timer = setInterval(() => loadStatus(selectedNodeId), 5000)
    return () => clearInterval(timer)
  }, [selectedNodeId, loadStatus])

  const handleStart = async () => {
    if (!selectedNodeId) return
    const key = 'start'
    setActionLoading(prev => ({ ...prev, [key]: true }))
    try {
      await startFilerNodeBalancer(selectedNodeId)
      message.success(`节点 ${selectedNodeId} 均衡器已启动`)
      loadStatus(selectedNodeId)
    } catch (error) {
      message.error(`节点 ${selectedNodeId} 启动均衡器失败`)
    } finally {
      setActionLoading(prev => ({ ...prev, [key]: false }))
    }
  }

  const handleStop = async () => {
    if (!selectedNodeId) return
    const key = 'stop'
    setActionLoading(prev => ({ ...prev, [key]: true }))
    try {
      await stopFilerNodeBalancer(selectedNodeId)
      message.success(`节点 ${selectedNodeId} 均衡器已停止`)
      loadStatus(selectedNodeId)
    } catch (error) {
      message.error(`节点 ${selectedNodeId} 停止均衡器失败`)
    } finally {
      setActionLoading(prev => ({ ...prev, [key]: false }))
    }
  }

  const handleTrigger = async () => {
    if (!selectedNodeId) return
    const key = 'trigger'
    setActionLoading(prev => ({ ...prev, [key]: true }))
    try {
      await triggerFilerNodeBalancer(selectedNodeId)
      message.success(`节点 ${selectedNodeId} 已触发均衡检查`)
      loadStatus(selectedNodeId)
    } catch (error) {
      message.error(`节点 ${selectedNodeId} 触发均衡检查失败`)
    } finally {
      setActionLoading(prev => ({ ...prev, [key]: false }))
    }
  }

  const handleConfigChange = async (key: keyof SchedulerConfig, value: number) => {
    if (!config || !selectedNodeId) return
    setConfigLoading(true)
    try {
      const newConfig = { ...config, [key]: value }
      await updateFilerNodeBalancerConfig(selectedNodeId, newConfig)
      setConfig(newConfig)
      message.success('配置已更新')
    } catch (error) {
      message.error('更新配置失败')
      // 失败时重新加载配置，回滚本地状态
      loadConfig(selectedNodeId)
    } finally {
      setConfigLoading(false)
    }
  }

  // ── 批量操作 (Phase C: 并发调所有 filer) ──
  const handleBatchAction = async (action: 'start' | 'stop' | 'trigger') => {
    const key = `batch-${action}`
    setBatchLoading(prev => ({ ...prev, [key]: true }))
    try {
      const result = action === 'start'
        ? await startAllBalancers()
        : action === 'stop'
        ? await stopAllBalancers()
        : await triggerAllBalancers()
      setBatchResult(result)
      setBatchAction(action)
      setBatchModalOpen(true)
      // 刷新当前选中节点状态
      if (selectedNodeId) loadStatus(selectedNodeId)
    } catch (error) {
      message.error(`批量${action === 'start' ? '启动' : action === 'stop' ? '停止' : '触发'}失败`)
    } finally {
      setBatchLoading(prev => ({ ...prev, [key]: false }))
    }
  }

  const getBalanceScore = () => {
    if (!status || status.node_count === 0) return 100
    const leaders = Object.values(status.leader_distribution)
    if (leaders.length === 0) return 100
    const avg = leaders.reduce((a, b) => a + b, 0) / leaders.length
    if (avg === 0) return 100
    const variance = leaders.reduce((sum, count) => sum + Math.pow(count - avg, 2), 0) / leaders.length
    const stdDev = Math.sqrt(variance)
    const imbalance = stdDev / avg
    return Math.max(0, 100 - imbalance * 50)
  }

  // Map raw Raft address (e.g. "172.30.0.32:8889") to node_id (e.g. "filer-2")
  const addrToNodeId = (addr: string): string => {
    const ip = addr.split(':')[0]
    const match = nodes.find(n => n.address === ip)
    return match ? match.node_id : addr
  }

  const balanceScore = getBalanceScore()
  const balanceColor = balanceScore >= 80 ? '#52c41a' : balanceScore >= 50 ? '#faad14' : '#ff4d4f'

  const successRate = status
    ? status.total_migrations > 0
      ? Math.round((status.successful_migrations / status.total_migrations) * 100)
      : 100
    : 100

  const selectedNode = nodes.find(n => n.node_id === selectedNodeId)
  const nodeOffline = selectedNode && !isNodeAlive(selectedNode.heartbeat_status)

  return (
    <Spin spinning={statusLoading && !status}>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <DatabaseOutlined style={{ fontSize: 24, color: 'var(--pf-color-primary)' }} />
          <Title level={4} style={{ margin: 0 }}>分片均衡</Title>
        </Space>
        <Space>
          {/* 批量操作 (Phase C: 并发调所有 filer) */}
          <Tooltip title="对所有 Filer 节点批量操作">
            <Space>
              <Popconfirm
                title="启动所有 Filer 节点的均衡器?"
                onConfirm={() => handleBatchAction('start')}
              >
                <Button
                  icon={<GlobalOutlined />}
                  loading={batchLoading['batch-start']}
                  disabled={nodes.length === 0}
                >
                  全部启动
                </Button>
              </Popconfirm>
              <Popconfirm
                title="停止所有 Filer 节点的均衡器?"
                onConfirm={() => handleBatchAction('stop')}
              >
                <Button
                  icon={<GlobalOutlined />}
                  danger
                  loading={batchLoading['batch-stop']}
                  disabled={nodes.length === 0}
                >
                  全部停止
                </Button>
              </Popconfirm>
              <Popconfirm
                title="对所有 Filer 节点触发均衡检查?"
                onConfirm={() => handleBatchAction('trigger')}
              >
                <Button
                  icon={<ThunderboltOutlined />}
                  loading={batchLoading['batch-trigger']}
                  disabled={nodes.length === 0}
                >
                  全部触发
                </Button>
              </Popconfirm>
            </Space>
          </Tooltip>
          <Divider type="vertical" />
          {/* 单节点操作 */}
          <Tooltip title="手动触发均衡检查">
            <Popconfirm
              title={`触发节点 ${selectedNodeId} 的均衡检查?`}
              onConfirm={handleTrigger}
              disabled={!selectedNodeId || !status?.is_running || nodeOffline}
            >
              <Button
                type="primary"
                icon={<ThunderboltOutlined />}
                disabled={!selectedNodeId || !status?.is_running || !!nodeOffline}
                loading={actionLoading.trigger}
              >
                手动触发
              </Button>
            </Popconfirm>
          </Tooltip>
          {status?.is_running ? (
            <Popconfirm title={`停止节点 ${selectedNodeId} 的均衡器?`} onConfirm={handleStop}>
              <Button icon={<StopOutlined />} loading={actionLoading.stop}>停止</Button>
            </Popconfirm>
          ) : (
            <Button
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={handleStart}
              disabled={!selectedNodeId || !!nodeOffline}
              loading={actionLoading.start}
            >
              启动
            </Button>
          )}
        </Space>
      </div>

      {/* ── 节点选择器 ── 按节点维度操作 balancer (Phase B) ── */}
      <Card size="small" style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <CloudServerOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary">Filer 节点:</Text>
          <Select
            style={{ width: 320 }}
            placeholder="选择 Filer 节点管理其均衡器"
            value={selectedNodeId ?? undefined}
            onChange={setSelectedNodeId}
            loading={nodesLoading}
            options={nodes.map(n => ({
              value: n.node_id,
              label: (
                <Space>
                  <Text>{n.node_id}</Text>
                  <Text type="secondary" style={{ fontSize: 12 }}>{n.address}:{n.http_port}</Text>
                  <Tag color={isNodeAlive(n.heartbeat_status) ? 'success' : 'error'} style={{ margin: 0, fontSize: 11 }}>
                    {isNodeAlive(n.heartbeat_status) ? '在线' : '离线'}
                  </Tag>
                </Space>
              ),
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
        <Card>
          <Empty description={nodesLoading ? '加载节点中...' : '请先选择一个 Filer 节点'} />
        </Card>
      ) : nodeOffline ? (
        <Card>
          <Alert
            type="warning"
            showIcon
            message={`节点 ${selectedNodeId} 心跳离线`}
            description="该节点当前不可达，均衡器操作不可用。请检查节点状态或选择其他在线节点。"
          />
        </Card>
      ) : (
        <>
          <Card size="small" style={{ marginBottom: 24 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
              <Text type="secondary" style={{ fontSize: 13 }}>
                分片均衡器会自动检测各节点的 Leader 分布情况，当发现负载不均衡时，会将过载节点的 Leader 迁移到负载较低的节点，
                从而保持集群性能稳定。当前管理节点 <Text strong>{selectedNodeId}</Text> 的均衡器。建议保持均衡器持续运行。
              </Text>
            </div>
          </Card>

          <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
            <Col xs={24} md={8}>
              <Card title="均衡器状态" size="small">
                <div style={{ textAlign: 'center', padding: '20px 0' }}>
                  <div style={{
                    width: 80, height: 80, borderRadius: '50%',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    margin: '0 auto 16px',
                    background: status?.is_running ? '#f6ffed' : '#fff2f0',
                  }}>
                    {status?.is_running ? (
                      <CheckCircleOutlined style={{ fontSize: 40, color: '#52c41a' }} />
                    ) : (
                      <AlertOutlined style={{ fontSize: 40, color: '#ff4d4f' }} />
                    )}
                  </div>
                  <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 8 }}>
                    {status?.is_running ? '运行中' : '已停止'}
                  </div>
                  <Tag color={status?.is_running ? 'green' : 'red'}>
                    {status?.is_running ? '自动均衡' : '手动模式'}
                  </Tag>
                </div>
              </Card>
            </Col>

            <Col xs={24} md={8}>
              <Card title="均衡度评分" size="small">
                <div style={{ padding: '20px 0' }}>
                  <div style={{ textAlign: 'center', marginBottom: 16 }}>
                    <span style={{ fontSize: 48, fontWeight: 700, color: balanceColor }}>
                      {Math.round(balanceScore)}
                    </span>
                    <span style={{ fontSize: 16, color: 'var(--pf-color-secondary)', marginLeft: 8 }}>分</span>
                  </div>
                  <Progress percent={Math.round(balanceScore)} strokeColor={balanceColor} showInfo={false} size="small" />
                  <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>
                    评分基于 Leader 在各节点的分布均匀度计算
                  </Text>
                </div>
              </Card>
            </Col>

            <Col xs={24} md={8}>
              <Card title="迁移统计" size="small">
                <div style={{ padding: '16px 0' }}>
                  <Row gutter={16}>
                    <Col span={8} style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 24, fontWeight: 700 }}>{status?.total_migrations || 0}</div>
                      <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>总迁移</div>
                    </Col>
                    <Col span={8} style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 24, fontWeight: 700, color: '#52c41a' }}>
                        {status?.successful_migrations || 0}
                      </div>
                      <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>成功</div>
                    </Col>
                    <Col span={8} style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 24, fontWeight: 700, color: '#ff4d4f' }}>
                        {status?.failed_migrations || 0}
                      </div>
                      <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>失败</div>
                    </Col>
                  </Row>
                  <div style={{ marginTop: 16 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <Text style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>成功率</Text>
                      <Text strong>{successRate}%</Text>
                    </div>
                    <Progress percent={successRate} strokeColor="#52c41a" showInfo={false} size="small" />
                  </div>
                </div>
              </Card>
            </Col>
          </Row>

          <Card title={`Leader 分布 (节点 ${selectedNodeId})`} size="small" style={{ marginBottom: 24 }}>
            {status?.leader_distribution && Object.keys(status.leader_distribution).length > 0 ? (
              <Row gutter={16}>
                {Object.entries(status.leader_distribution).map(([addr, count]) => {
                  const nodeId = addrToNodeId(addr)
                  return (
                    <Col xs={12} md={6} key={addr}>
                      <div style={{
                        padding: 16, borderRadius: 8,
                        background: 'var(--pf-color-bg-container)',
                        border: '1px solid var(--pf-color-border)',
                      }}>
                        <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)', marginBottom: 8 }}>
                          {nodeId}
                        </div>
                        <div style={{ fontSize: 32, fontWeight: 700, marginBottom: 8 }}>{count}</div>
                        <Progress
                          percent={(count / (status.shard_count || 1)) * 100}
                          showInfo={false}
                          size="small"
                        />
                      </div>
                    </Col>
                  )
                })}
              </Row>
            ) : (
              <Empty description="暂无分布数据" />
            )}
          </Card>

          <Card title={`均衡器配置 (节点 ${selectedNodeId})`} size="small">
            <Spin spinning={configLoading}>
              {config ? (
                <Space direction="vertical" size={24} style={{ width: '100%' }}>
                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <Text>检查间隔</Text>
                      <Text type="secondary">{config.check_interval} 秒</Text>
                    </div>
                    <Slider
                      min={30}
                      max={600}
                      step={10}
                      value={config.check_interval}
                      onChange={(value) => handleConfigChange('check_interval', value)}
                      style={{ marginBottom: 8 }}
                    />
                    <Text type="secondary" style={{ fontSize: 12 }}>均衡器每隔指定时间检查一次负载分布</Text>
                  </div>

                  <Divider />

                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <Text>每轮最大迁移数</Text>
                      <Text type="secondary">{config.max_transfers_per_round}</Text>
                    </div>
                    <Slider
                      min={1}
                      max={10}
                      value={config.max_transfers_per_round}
                      onChange={(value) => handleConfigChange('max_transfers_per_round', value)}
                      style={{ marginBottom: 8 }}
                    />
                    <Text type="secondary" style={{ fontSize: 12 }}>单次均衡检查最多迁移的 Leader 数量</Text>
                  </div>

                  <Divider />

                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <Text>迁移间隔</Text>
                      <Text type="secondary">{config.transfer_interval} 秒</Text>
                    </div>
                    <Slider
                      min={5}
                      max={120}
                      step={5}
                      value={config.transfer_interval}
                      onChange={(value) => handleConfigChange('transfer_interval', value)}
                      style={{ marginBottom: 8 }}
                    />
                    <Text type="secondary" style={{ fontSize: 12 }}>两次迁移之间的等待时间，避免频繁迁移</Text>
                  </div>

                  <Divider />

                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <Text>冷却期</Text>
                      <Text type="secondary">{config.cooldown_periods} 轮</Text>
                    </div>
                    <Slider
                      min={1}
                      max={10}
                      value={config.cooldown_periods}
                      onChange={(value) => handleConfigChange('cooldown_periods', value)}
                      style={{ marginBottom: 8 }}
                    />
                    <Text type="secondary" style={{ fontSize: 12 }}>迁移完成后，经过多少轮检查才能再次迁移</Text>
                  </div>

                  <Divider />

                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <Text>Leader 不平衡阈值</Text>
                      <Text type="secondary">{config.leader_imbalance_threshold}</Text>
                    </div>
                    <Slider
                      min={1}
                      max={5}
                      step={0.1}
                      value={config.leader_imbalance_threshold}
                      onChange={(value) => handleConfigChange('leader_imbalance_threshold', value)}
                      style={{ marginBottom: 8 }}
                    />
                    <Text type="secondary" style={{ fontSize: 12 }}>当各节点 Leader 数量差异超过此阈值时触发均衡（1.5 = 平均值的 1.5 倍）</Text>
                  </div>

                  <Divider />

                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <Text>CPU 阈值</Text>
                      <Text type="secondary">{(config.cpu_threshold * 100).toFixed(0)}%</Text>
                    </div>
                    <Slider
                      min={0.5}
                      max={0.95}
                      step={0.05}
                      value={config.cpu_threshold}
                      onChange={(value) => handleConfigChange('cpu_threshold', value)}
                      style={{ marginBottom: 8 }}
                    />
                    <Text type="secondary" style={{ fontSize: 12 }}>节点 CPU 使用率超过此阈值时，不会接收新的 Leader</Text>
                  </div>

                  <Divider />

                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <Text>内存阈值</Text>
                      <Text type="secondary">{(config.memory_threshold * 100).toFixed(0)}%</Text>
                    </div>
                    <Slider
                      min={0.5}
                      max={0.95}
                      step={0.05}
                      value={config.memory_threshold}
                      onChange={(value) => handleConfigChange('memory_threshold', value)}
                      style={{ marginBottom: 8 }}
                    />
                    <Text type="secondary" style={{ fontSize: 12 }}>节点内存使用率超过此阈值时，不会接收新的 Leader</Text>
                  </div>

                  <Divider />

                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <Text>磁盘阈值</Text>
                      <Text type="secondary">{(config.disk_threshold * 100).toFixed(0)}%</Text>
                    </div>
                    <Slider
                      min={0.5}
                      max={0.95}
                      step={0.05}
                      value={config.disk_threshold}
                      onChange={(value) => handleConfigChange('disk_threshold', value)}
                      style={{ marginBottom: 8 }}
                    />
                    <Text type="secondary" style={{ fontSize: 12 }}>节点磁盘使用率超过此阈值时，不会接收新的 Leader</Text>
                  </div>
                </Space>
              ) : (
                <Empty description="加载配置中..." />
              )}
            </Spin>
          </Card>
        </>
      )}

      <Card title="常见问题" size="small" style={{ marginTop: 24 }}>
        <Descriptions column={1} size="small">
          <Descriptions.Item label="什么是分片均衡？">
            分片均衡是指将各分片的 Leader 角色均匀分配到集群中的各个节点，避免某些节点负载过重。
          </Descriptions.Item>
          <Descriptions.Item label="为什么需要均衡？">
            Leader 节点负责处理所有写入请求，如果 Leader 集中在少数节点上，这些节点会成为性能瓶颈。
          </Descriptions.Item>
          <Descriptions.Item label="均衡过程会影响业务吗？">
            Leader 迁移过程是平滑的，系统会先确保数据同步完成再切换角色，不会造成数据丢失。
          </Descriptions.Item>
          <Descriptions.Item label="什么时候需要手动触发？">
            在节点扩容、缩容或出现故障恢复后，可以手动触发一次均衡检查，快速恢复集群平衡。
          </Descriptions.Item>
          <Descriptions.Item label="为什么按节点操作？">
            多 Filer 集群中，每个 Filer 节点独立运行自己的均衡器实例。按节点操作可以针对特定节点启停均衡器，
            而不影响其他节点。集群级批量操作（start/stop all）将在 Phase C 提供。
          </Descriptions.Item>
        </Descriptions>
      </Card>

      {/* ── 批量操作结果 Modal (Phase C) ── */}
      <Modal
        title={
          <Space>
            <GlobalOutlined />
            <span>批量{batchAction === 'start' ? '启动' : batchAction === 'stop' ? '停止' : '触发'}结果</span>
          </Space>
        }
        open={batchModalOpen}
        onCancel={() => setBatchModalOpen(false)}
        footer={<Button type="primary" onClick={() => setBatchModalOpen(false)}>关闭</Button>}
      >
        {batchResult && (
          <div>
            <Row gutter={16} style={{ marginBottom: 16, textAlign: 'center' }}>
              <Col span={8}>
                <Statistic
                  title="总节点数"
                  value={batchResult.total}
                  valueStyle={{ fontSize: 20 }}
                />
              </Col>
              <Col span={8}>
                <Statistic
                  title="成功"
                  value={batchResult.success.length}
                  valueStyle={{ fontSize: 20, color: '#52c41a' }}
                  prefix={<CheckCircleOutlined />}
                />
              </Col>
              <Col span={8}>
                <Statistic
                  title="失败"
                  value={batchResult.failed.length}
                  valueStyle={{ fontSize: 20, color: batchResult.failed.length > 0 ? '#ff4d4f' : undefined }}
                  prefix={batchResult.failed.length > 0 ? <AlertOutlined /> : undefined}
                />
              </Col>
            </Row>
            {batchResult.failed.length > 0 && (
              <>
                <Divider style={{ margin: '12px 0' }} />
                <Text strong style={{ color: '#ff4d4f' }}>失败节点:</Text>
                <List
                  size="small"
                  bordered
                  style={{ marginTop: 8 }}
                  dataSource={batchResult.failed}
                  renderItem={(item) => (
                    <List.Item>
                      <Space>
                        <Tag color="error">{item.node_id}</Tag>
                        <Text type="secondary" style={{ fontSize: 12 }}>{item.error}</Text>
                      </Space>
                    </List.Item>
                  )}
                />
              </>
            )}
            {batchResult.success.length > 0 && (
              <>
                <Divider style={{ margin: '12px 0' }} />
                <Text strong style={{ color: '#52c41a' }}>成功节点:</Text>
                <div style={{ marginTop: 8, display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                  {batchResult.success.map(id => (
                    <Tag color="success" key={id}>{id}</Tag>
                  ))}
                </div>
              </>
            )}
          </div>
        )}
      </Modal>
    </Spin>
  )
}

export default ShardBalancing
