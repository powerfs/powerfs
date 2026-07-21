import { useState, useEffect } from 'react'
import { Card, Table, Tag, Statistic, Row, Col, Spin, message, Tooltip, Empty, Space, Typography, Descriptions } from 'antd'
import {
  CloudServerOutlined,
  DatabaseOutlined,
  FileOutlined,
  FolderOutlined,
  ThunderboltOutlined,
  ReloadOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons'
import type { FilerStatus } from '@/types'
import { getFilerStatus } from '@/services/api'

const { Text } = Typography

function Filer() {
  const [status, setStatus] = useState<FilerStatus | null>(null)
  const [loading, setLoading] = useState(true)

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

  useEffect(() => {
    loadStatus()
    const timer = setInterval(loadStatus, 10000)
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
