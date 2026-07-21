import { useState, useEffect } from 'react'
import {
  Card, Spin, Typography, Space, Row, Col, Button, Tag, Progress, Slider,
  Divider, Descriptions, Empty, message,
} from 'antd'
import {
  DatabaseOutlined, PlayCircleOutlined, StopOutlined, ReloadOutlined,
  InfoCircleOutlined, CheckCircleOutlined, AlertOutlined,
} from '@ant-design/icons'
import {
  getBalancerStatus, startBalancer, stopBalancer, triggerBalance,
  getBalancerConfig, setBalancerConfig,
  type SchedulerStatus, type SchedulerConfig,
} from '@/services/api'

const { Text, Title } = Typography

function ShardBalancing() {
  const [status, setStatus] = useState<SchedulerStatus | null>(null)
  const [config, setConfig] = useState<SchedulerConfig | null>(null)
  const [loading, setLoading] = useState(true)
  const [configLoading, setConfigLoading] = useState(false)

  const loadStatus = async () => {
    setLoading(true)
    try {
      const data = await getBalancerStatus()
      setStatus(data)
    } catch (error) {
      console.error('Failed to load balancer status:', error)
      message.error('加载均衡器状态失败')
    } finally {
      setLoading(false)
    }
  }

  const loadConfig = async () => {
    try {
      const data = await getBalancerConfig()
      setConfig(data)
    } catch (error) {
      console.error('Failed to load balancer config:', error)
    }
  }

  useEffect(() => {
    loadStatus()
    loadConfig()
    const timer = setInterval(loadStatus, 5000)
    return () => clearInterval(timer)
  }, [])

  const handleStart = async () => {
    try {
      await startBalancer()
      message.success('均衡器已启动')
      loadStatus()
    } catch (error) {
      message.error('启动均衡器失败')
    }
  }

  const handleStop = async () => {
    try {
      await stopBalancer()
      message.success('均衡器已停止')
      loadStatus()
    } catch (error) {
      message.error('停止均衡器失败')
    }
  }

  const handleTrigger = async () => {
    try {
      await triggerBalance()
      message.success('手动触发均衡检查')
      loadStatus()
    } catch (error) {
      message.error('触发均衡检查失败')
    }
  }

  const handleConfigChange = async (key: keyof SchedulerConfig, value: number) => {
    if (!config) return
    setConfigLoading(true)
    try {
      const newConfig = { ...config, [key]: value }
      await setBalancerConfig(newConfig)
      setConfig(newConfig)
      message.success('配置已更新')
    } catch (error) {
      message.error('更新配置失败')
    } finally {
      setConfigLoading(false)
    }
  }

  const getBalanceScore = () => {
    if (!status || status.node_count === 0) return 100
    const leaders = Object.values(status.leader_distribution)
    if (leaders.length === 0) return 100
    const avg = leaders.reduce((a, b) => a + b, 0) / leaders.length
    const variance = leaders.reduce((sum, count) => sum + Math.pow(count - avg, 2), 0) / leaders.length
    const stdDev = Math.sqrt(variance)
    const imbalance = stdDev / avg
    return Math.max(0, 100 - imbalance * 50)
  }

  const balanceScore = getBalanceScore()
  const balanceColor = balanceScore >= 80 ? '#52c41a' : balanceScore >= 50 ? '#faad14' : '#ff4d4f'

  const successRate = status
    ? status.total_migrations > 0
      ? Math.round((status.successful_migrations / status.total_migrations) * 100)
      : 100
    : 100

  return (
    <Spin spinning={loading}>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <DatabaseOutlined style={{ fontSize: 24, color: 'var(--pf-color-primary)' }} />
          <Title level={4} style={{ margin: 0 }}>分片均衡</Title>
        </Space>
        <Space>
          <Button
            type="primary"
            icon={<ReloadOutlined />}
            onClick={handleTrigger}
            disabled={!status?.is_running}
          >
            手动触发
          </Button>
          {status?.is_running ? (
            <Button icon={<StopOutlined />} onClick={handleStop}>
              停止
            </Button>
          ) : (
            <Button type="primary" icon={<PlayCircleOutlined />} onClick={handleStart}>
              启动
            </Button>
          )}
        </Space>
      </div>

      <Card size="small" style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            分片均衡器会自动检测各节点的 Leader 分布情况，当发现负载不均衡时，会将过载节点的 Leader 迁移到负载较低的节点，
            从而保持集群性能稳定。建议保持均衡器持续运行。
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
              }}
              >
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
              <Progress
                percent={Math.round(balanceScore)}
                strokeColor={balanceColor}
                showInfo={false}
                size="small"
              />
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

      <Card title="Leader 分布" size="small" style={{ marginBottom: 24 }}>
        {status?.leader_distribution && Object.keys(status.leader_distribution).length > 0 ? (
          <Row gutter={16}>
            {Object.entries(status.leader_distribution).map(([node, count]) => (
              <Col xs={12} md={6} key={node}>
                <div style={{
                  padding: 16, borderRadius: 8,
                  background: 'var(--pf-color-bg-container)',
                  border: '1px solid var(--pf-color-border)',
                }}
                >
                  <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)', marginBottom: 8 }}>
                    {node}
                  </div>
                  <div style={{ fontSize: 32, fontWeight: 700, marginBottom: 8 }}>{count}</div>
                  <Progress
                    percent={(count / (status.shard_count || 1)) * 100}
                    showInfo={false}
                    size="small"
                  />
                </div>
              </Col>
            ))}
          </Row>
        ) : (
          <Empty description="暂无分布数据" />
        )}
      </Card>

      <Card title="均衡器配置" size="small">
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
                  <Text type="secondary">{config.leader_imbalance_threshold}%</Text>
                </div>
                <Slider
                  min={10}
                  max={50}
                  step={5}
                  value={config.leader_imbalance_threshold}
                  onChange={(value) => handleConfigChange('leader_imbalance_threshold', value)}
                  style={{ marginBottom: 8 }}
                />
                <Text type="secondary" style={{ fontSize: 12 }}>当各节点 Leader 数量差异超过此阈值时触发均衡</Text>
              </div>

              <Divider />

              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <Text>CPU 阈值</Text>
                  <Text type="secondary">{config.cpu_threshold}%</Text>
                </div>
                <Slider
                  min={50}
                  max={95}
                  step={5}
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
                  <Text type="secondary">{config.memory_threshold}%</Text>
                </div>
                <Slider
                  min={50}
                  max={95}
                  step={5}
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
                  <Text type="secondary">{config.disk_threshold}%</Text>
                </div>
                <Slider
                  min={50}
                  max={95}
                  step={5}
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
        </Descriptions>
      </Card>
    </Spin>
  )
}

export default ShardBalancing