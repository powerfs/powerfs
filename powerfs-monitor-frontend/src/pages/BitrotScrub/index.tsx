import { useEffect, useState } from 'react'
import {
  Card,
  Table,
  Tag,
  Button,
  Modal,
  Space,
  Progress,
  Tooltip,
  Statistic,
  Row,
  Col,
  message,
  Typography,
  List,
} from 'antd'
import {
  SafetyCertificateOutlined,
  WarningOutlined,
  CheckCircleOutlined,
  SyncOutlined,
  ScanOutlined,
  ExclamationCircleOutlined,
  ClockCircleOutlined,
  EyeOutlined,
} from '@ant-design/icons'
import type { VolumeScrubStatus, ScrubSummary } from '@/types'
import { getScrubStatuses, getScrubSummary, triggerScrubVolume, triggerScrubAll } from '@/services/api'
import { formatNumber } from '@/utils/format'

const { Text } = Typography

const stateColors: Record<string, string> = {
  idle: 'default',
  running: 'processing',
  paused: 'warning',
  completed: 'success',
  failed: 'error',
}

const stateIcons: Record<string, React.ReactNode> = {
  idle: <ClockCircleOutlined />,
  running: <SyncOutlined spin />,
  paused: <ExclamationCircleOutlined />,
  completed: <CheckCircleOutlined />,
  failed: <WarningOutlined />,
}

const stateLabels: Record<string, string> = {
  idle: '空闲',
  running: '扫描中',
  paused: '已暂停',
  completed: '已完成',
  failed: '失败',
}

function formatTime(time?: string): string {
  if (!time) return '-'
  try {
    return new Date(time).toLocaleString('zh-CN')
  } catch {
    return time
  }
}

function BitrotScrub() {
  const [statuses, setStatuses] = useState<VolumeScrubStatus[]>([])
  const [summary, setSummary] = useState<ScrubSummary | null>(null)
  const [loading, setLoading] = useState(false)
  const [selectedVolume, setSelectedVolume] = useState<VolumeScrubStatus | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [showTriggerAll, setShowTriggerAll] = useState(false)

  const loadData = async () => {
    setLoading(true)
    try {
      const [statusData, summaryData] = await Promise.all([
        getScrubStatuses(),
        getScrubSummary(),
      ])
      setStatuses(statusData)
      setSummary(summaryData)
    } catch (e) {
      console.error('Failed to load scrub data:', e)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadData()
    const interval = setInterval(loadData, 10000)
    return () => clearInterval(interval)
  }, [])

  const handleTriggerScrub = async (volumeId: number) => {
    try {
      await triggerScrubVolume(volumeId)
      message.success(`Volume ${volumeId} 扫描已触发`)
      loadData()
    } catch {
      message.error('触发扫描失败')
    }
  }

  const handleTriggerAll = async () => {
    try {
      await triggerScrubAll()
      message.success('全量扫描已触发')
      setShowTriggerAll(false)
      loadData()
    } catch {
      message.error('触发全量扫描失败')
    }
  }

  const handleViewDetail = (record: VolumeScrubStatus) => {
    setSelectedVolume(record)
    setShowDetail(true)
  }

  const columns = [
    {
      title: 'Volume ID',
      dataIndex: 'volume_id',
      key: 'volume_id',
      width: 100,
      render: (id: number) => <strong>{id}</strong>,
    },
    {
      title: '状态',
      dataIndex: 'state',
      key: 'state',
      width: 120,
      render: (state: string) => (
        <Tag color={stateColors[state]} icon={stateIcons[state]}>
          {stateLabels[state] || state}
        </Tag>
      ),
    },
    {
      title: '进度',
      dataIndex: 'progress',
      key: 'progress',
      width: 180,
      render: (progress: number, record: VolumeScrubStatus) => (
        <Tooltip title={`${formatNumber(record.verified_needles)} / ${formatNumber(record.total_needles)}`}>
          <Progress
            percent={Math.round(progress * 100)}
            size="small"
            status={
              record.state === 'failed' ? 'exception' :
              record.state === 'completed' ? 'success' :
              record.state === 'running' ? 'active' :
              'normal'
            }
          />
        </Tooltip>
      ),
    },
    {
      title: '总数',
      dataIndex: 'total_needles',
      key: 'total_needles',
      width: 100,
      render: (v: number) => formatNumber(v),
    },
    {
      title: '已校验',
      dataIndex: 'verified_needles',
      key: 'verified_needles',
      width: 100,
      render: (v: number) => <span style={{ color: '#52c41a' }}>{formatNumber(v)}</span>,
    },
    {
      title: '损坏',
      dataIndex: 'corrupted_needles',
      key: 'corrupted_needles',
      width: 80,
      render: (v: number) =>
        v > 0 ? (
          <span style={{ color: '#ff4d4f', fontWeight: 'bold' }}>{formatNumber(v)}</span>
        ) : (
          <span style={{ color: '#52c41a' }}>0</span>
        ),
    },
    {
      title: '跳过',
      dataIndex: 'skipped_needles',
      key: 'skipped_needles',
      width: 80,
      render: (v: number) => formatNumber(v),
    },
    {
      title: '上次扫描',
      dataIndex: 'last_scrub_at',
      key: 'last_scrub_at',
      width: 180,
      render: (time?: string) => formatTime(time),
    },
    {
      title: '操作',
      key: 'actions',
      width: 200,
      render: (_: unknown, record: VolumeScrubStatus) => (
        <Space>
          <Button
            size="small"
            icon={<EyeOutlined />}
            onClick={() => handleViewDetail(record)}
          >
            详情
          </Button>
          <Button
            size="small"
            type="primary"
            icon={<ScanOutlined />}
            disabled={record.state === 'running'}
            onClick={() => handleTriggerScrub(record.volume_id)}
          >
            扫描
          </Button>
        </Space>
      ),
    },
  ]

  return (
    <div>
      {/* 统计卡片 */}
      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col span={4}>
          <Card>
            <Statistic
              title="Volume 总数"
              value={summary?.total_volumes ?? 0}
              prefix={<SafetyCertificateOutlined />}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card>
            <Statistic
              title="已扫描"
              value={summary?.scanned_volumes ?? 0}
              valueStyle={{ color: '#1890ff' }}
              prefix={<ScanOutlined />}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card>
            <Statistic
              title="健康"
              value={summary?.healthy_volumes ?? 0}
              valueStyle={{ color: '#52c41a' }}
              prefix={<CheckCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card>
            <Statistic
              title="有损坏"
              value={summary?.corrupted_volumes ?? 0}
              valueStyle={{ color: '#ff4d4f' }}
              prefix={<WarningOutlined />}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card>
            <Statistic
              title="总 Needle 数"
              value={summary?.total_needles ?? 0}
              formatter={(v) => formatNumber(Number(v))}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card>
            <Statistic
              title="损坏 Needle"
              value={summary?.corrupted_needles ?? 0}
              valueStyle={{ color: summary && summary.corrupted_needles > 0 ? '#ff4d4f' : '#52c41a' }}
              prefix={<ExclamationCircleOutlined />}
            />
          </Card>
        </Col>
      </Row>

      {/* 操作栏 */}
 <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: 18, fontWeight: 500 }}>Bitrot 扫描状态</span>
        <Space>
          <Button icon={<SyncOutlined />} onClick={loadData} loading={loading}>
            刷新
          </Button>
          <Button
            type="primary"
            icon={<ScanOutlined />}
            onClick={() => setShowTriggerAll(true)}
          >
            全量扫描
          </Button>
        </Space>
      </div>

      {/* 状态表格 */}
      <Card>
        <Table
          columns={columns}
          dataSource={statuses}
          rowKey="volume_id"
          loading={loading}
          pagination={{ pageSize: 10 }}
          size="middle"
          rowClassName={(record) =>
            record.corrupted_needles > 0 ? 'corrupted-row' : ''
          }
        />
      </Card>

      {/* Volume 详情弹窗 */}
      <Modal
        title={`Volume ${selectedVolume?.volume_id} - Bitrot 扫描详情`}
        open={showDetail}
        onCancel={() => setShowDetail(false)}
        footer={[
          <Button key="close" onClick={() => setShowDetail(false)}>
            关闭
          </Button>,
          selectedVolume && selectedVolume.state !== 'running' && (
            <Button
              key="scan"
              type="primary"
              icon={<ScanOutlined />}
              onClick={() => {
                handleTriggerScrub(selectedVolume.volume_id)
                setShowDetail(false)
              }}
            >
              重新扫描
            </Button>
          ),
        ]}
        width={640}
      >
        {selectedVolume && (
          <div>
            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col span={8}>
                <Statistic
                  title="状态"
                  value={stateLabels[selectedVolume.state] || selectedVolume.state}
                />
              </Col>
              <Col span={8}>
                <Statistic title="进度" value={`${Math.round(selectedVolume.progress * 100)}%`} />
              </Col>
              <Col span={8}>
                <Statistic title="损坏数" value={selectedVolume.corrupted_needles} valueStyle={{ color: selectedVolume.corrupted_needles > 0 ? '#ff4d4f' : '#52c41a' }} />
              </Col>
            </Row>

            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col span={6}>
                <Statistic title="总 Needle" value={selectedVolume.total_needles} />
              </Col>
              <Col span={6}>
                <Statistic title="已校验" value={selectedVolume.verified_needles} valueStyle={{ color: '#52c41a' }} />
              </Col>
              <Col span={6}>
                <Statistic title="跳过" value={selectedVolume.skipped_needles} />
              </Col>
              <Col span={6}>
                <Statistic title="错误" value={selectedVolume.error_needles} valueStyle={{ color: selectedVolume.error_needles > 0 ? '#ff4d4f' : undefined }} />
              </Col>
            </Row>

            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col span={8}>
                <Text type="secondary">开始时间：</Text>
                <br />
                <Text>{formatTime(selectedVolume.started_at)}</Text>
              </Col>
              <Col span={8}>
                <Text type="secondary">完成时间：</Text>
                <br />
                <Text>{formatTime(selectedVolume.completed_at)}</Text>
              </Col>
              <Col span={8}>
                <Text type="secondary">上次扫描：</Text>
                <br />
                <Text>{formatTime(selectedVolume.last_scrub_at)}</Text>
              </Col>
            </Row>

            {selectedVolume.error && (
              <Card size="small" style={{ marginBottom: 16, borderColor: '#ff4d4f' }}>
                <Text type="danger" strong>
                  <WarningOutlined /> 错误信息：
                </Text>
                <br />
                <Text type="danger">{selectedVolume.error}</Text>
              </Card>
            )}

            {selectedVolume.corrupted_needle_ids && selectedVolume.corrupted_needle_ids.length > 0 && (
              <Card size="small" title={`损坏的 Needle 列表 (${selectedVolume.corrupted_needle_ids.length})`}>
                <List
                  size="small"
                  dataSource={selectedVolume.corrupted_needle_ids}
                  renderItem={(id) => (
                    <List.Item>
                      <Text type="danger">
                        <ExclamationCircleOutlined /> Needle ID: {id}
                      </Text>
                    </List.Item>
                  )}
                  pagination={{ pageSize: 5 }}
                />
              </Card>
            )}
          </div>
        )}
      </Modal>

      {/* 全量扫描确认 */}
      <Modal
        title="全量扫描确认"
        open={showTriggerAll}
        onCancel={() => setShowTriggerAll(false)}
        onOk={handleTriggerAll}
        okText="确认扫描"
        cancelText="取消"
      >
        <p>
          <ExclamationCircleOutlined style={{ color: '#faad14', marginRight: 8 }} />
          确定要触发所有 Volume 的 Bitrot 扫描吗？此操作可能会产生 IO 负载。
        </p>
        <p>
          <Text type="secondary">
            共 {statuses.length} 个 Volume，其中{' '}
            {statuses.filter((s) => s.state === 'running').length} 个正在扫描中。
          </Text>
        </p>
      </Modal>
    </div>
  )
}

export default BitrotScrub
