import { useState, useEffect } from 'react'
import { Card, Table, Tag, Statistic, Row, Col, Spin, message, Tooltip, Empty, Space, Typography } from 'antd'
import {
  CloudServerOutlined,
  DatabaseOutlined,
  FileOutlined,
  FolderOutlined,
  ThunderboltOutlined,
  ReloadOutlined,
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
    </Spin>
  )
}

export default Filer
