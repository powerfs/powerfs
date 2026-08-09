import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Card, Table, Tag, Statistic, Row, Col, Spin, message, Tooltip, Empty, Space, Typography, Descriptions, Alert, Progress } from 'antd'
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
} from '@ant-design/icons'
import type { FilerStatus, ConflictStats, ConflictRecord } from '@/types'
import { getFilerStatus, getConflictStats, getConflicts } from '@/services/api'

const { Text, Link: TypographyLink } = Typography

function Filer() {
  const [status, setStatus] = useState<FilerStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [conflictStats, setConflictStats] = useState<ConflictStats | null>(null)
  const [recentConflicts, setRecentConflicts] = useState<ConflictRecord[]>([])
  const [conflictLoading, setConflictLoading] = useState(false)
  const navigate = useNavigate()

  const loadStatus = async () => {
    setLoading(true)
    try {
      const data = await getFilerStatus()
      setStatus(data)
    } catch (error) {
      console.error('Failed to load filer status:', error)
      message.error('加载Filer状态失败')
    } finally {
      setLoading(false)
    }
  }

  const loadConflictHealth = async () => {
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
  }

  useEffect(() => {
    loadStatus()
    loadConflictHealth()
    const timer = setInterval(() => {
      loadStatus()
      loadConflictHealth()
    }, 10000)
    return () => clearInterval(timer)
  }, [])

  const bucketColumns = [
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
    {
      title: '状态',
      key: 'status',
      width: 120,
      render: () => <Tag color="success">活跃</Tag>,
    },
  ]

  const buckets = (status?.buckets ?? []).map((name) => ({ key: name, name }))

  return (
    <Spin spinning={loading}>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <CloudServerOutlined style={{ fontSize: 24, color: 'var(--pf-color-primary)' }} />
          <Typography.Title level={4} style={{ margin: 0 }}>Filer 管理</Typography.Title>
        </Space>
        <Tooltip title="刷新">
          <ReloadOutlined onClick={loadStatus} style={{ fontSize: 16, cursor: 'pointer', color: 'var(--pf-color-primary)' }} />
        </Tooltip>
      </div>

      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            Filer 是 PowerFS 的文件系统元数据管理组件，负责管理文件和目录的元数据、处理文件系统操作请求。
            它将元数据分片存储，通过 Raft 协议保证数据一致性。
          </Text>
        </div>
      </Card>

      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col xs={12} sm={8} md={4}>
          <Card>
            <Statistic
              title="分片总数"
              value={status?.shard_count ?? 0}
              prefix={<DatabaseOutlined />}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card>
            <Statistic
              title="Leader 分片"
              value={status?.leader_count ?? 0}
              valueStyle={{ color: 'var(--pf-color-success)' }}
              prefix={<ThunderboltOutlined />}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card>
            <Statistic
              title="Inode 总数"
              value={status?.total_inodes ?? 0}
              prefix={<FileOutlined />}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card>
            <Statistic
              title="文件数"
              value={status?.total_files ?? 0}
              prefix={<FileOutlined />}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card>
            <Statistic
              title="目录数"
              value={status?.total_dirs ?? 0}
              prefix={<FolderOutlined />}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card>
            <Statistic
              title="Bucket 数"
              value={status?.buckets?.length ?? 0}
              prefix={<DatabaseOutlined />}
            />
          </Card>
        </Col>
      </Row>

      <Card title="Bucket 列表" extra={
        <Tag color={status ? 'success' : 'default'}>
          {status ? 'Filer 在线' : 'Filer 离线'}
        </Tag>
      }>
        {buckets.length > 0 ? (
          <Table
            columns={bucketColumns}
            dataSource={buckets}
            pagination={{ pageSize: 10 }}
            size="middle"
          />
        ) : (
          <Empty description="暂无Bucket" />
        )}
      </Card>

      {/* ═══════════ CRDT Conflict Health (Filer sub-page indicator, P2-C) ═══════════ */}
      <Card
        style={{ marginTop: 24 }}
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
            <TypographyLink
              onClick={() => navigate('/conflicts')}
              style={{ fontSize: 12 }}
            >
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
                    <Statistic
                      title="累计冲突"
                      value={conflictStats.total_count}
                      valueStyle={{ fontSize: 16 }}
                      prefix={<WarningOutlined />}
                    />
                  </Card>
                </Col>
                <Col xs={12} sm={8} md={4}>
                  <Card size="small" variant="outlined">
                    <Statistic
                      title="待处理"
                      value={conflictStats.unresolved_count}
                      valueStyle={{
                        color: conflictStats.unresolved_count > 0
                          ? 'var(--pf-color-warning)'
                          : 'var(--pf-color-success)',
                        fontSize: 16,
                      }}
                      prefix={<WarningOutlined />}
                      suffix={
                        conflictStats.unresolved_count > 0 ? '' : '✓'
                      }
                    />
                  </Card>
                </Col>
                <Col xs={12} sm={8} md={4}>
                  <Card size="small" variant="outlined">
                    <Statistic
                      title="已解决"
                      value={conflictStats.resolved_count}
                      valueStyle={{ color: 'var(--pf-color-success)', fontSize: 16 }}
                      prefix={<SafetyCertificateOutlined />}
                    />
                  </Card>
                </Col>
                <Col xs={24} sm={24} md={12}>
                  <Card size="small" variant="outlined">
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      解决率 ({conflictStats.total_count === 0
                        ? '—'
                        : `${Math.round((conflictStats.resolved_count / conflictStats.total_count) * 100)}%`})
                    </Text>
                    <Progress
                      percent={
                        conflictStats.total_count === 0
                          ? 100
                          : Math.round((conflictStats.resolved_count / conflictStats.total_count) * 100)
                      }
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
                <div style={{ fontWeight: 500, marginBottom: 8 }}>
                  最近 {recentConflicts.length} 条冲突记录
                </div>
                <Table
                  size="small"
                  rowKey="id"
                  pagination={false}
                  dataSource={recentConflicts}
                  columns={[
                    {
                      title: '冲突 ID',
                      dataIndex: 'id',
                      key: 'id',
                      width: 130,
                      render: (id: string) => id.slice(0, 12) + '…',
                    },
                    {
                      title: '类型',
                      dataIndex: 'conflict_type',
                      key: 'type',
                      width: 100,
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
                    {
                      title: '路径',
                      dataIndex: 'dir_path',
                      key: 'path',
                      render: (p: string) => p || <Text type="secondary">/</Text>,
                    },
                    {
                      title: '状态',
                      key: 'st',
                      width: 90,
                      render: (_: unknown, r: ConflictRecord) =>
                        r.resolved
                          ? <Tag color="success">resolved</Tag>
                          : <Tag color="warning">pending</Tag>,
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

      <Card title="常见问题" size="small" style={{ marginTop: 24 }}>
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
        </Descriptions>
      </Card>
    </Spin>
  )
}

export default Filer
