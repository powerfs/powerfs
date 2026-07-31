import { useState, useEffect } from 'react'
import {
  Card,
  Table,
  Tag,
  Button,
  Modal,
  Form,
  Input,
  InputNumber,
  Space,
  Popconfirm,
  message,
  Tooltip,
  Typography,
  Descriptions,
} from 'antd'
import {
  PlusOutlined,
  ReloadOutlined,
  DeleteOutlined,
  InfoCircleOutlined,
  DatabaseOutlined,
} from '@ant-design/icons'
import type { CollectionInfo } from '@/types'
import {
  getCollections,
  createCollection,
  deleteCollection,
  type CreateCollectionParams,
} from '@/services/api'
import { formatNumber } from '@/utils/format'

const { Text } = Typography

function formatTimestamp(ts: number): string {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString()
}

function Collections() {
  const [collections, setCollections] = useState<CollectionInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [createModalVisible, setCreateModalVisible] = useState(false)
  const [detailRecord, setDetailRecord] = useState<CollectionInfo | null>(null)
  const [form] = Form.useForm()

  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    setLoading(true)
    try {
      const list = await getCollections()
      setCollections(list)
    } catch (error) {
      console.error('Failed to load collections:', error)
      message.error('加载 Collection 列表失败')
    } finally {
      setLoading(false)
    }
  }

  const handleCreate = async () => {
    try {
      const values = await form.validateFields()
      const params: CreateCollectionParams = {
        name: values.name,
        replication: values.replication,
        ttl: values.ttl,
        disk_type: values.disk_type,
        max_volume_count: values.max_volume_count,
      }
      await createCollection(params)
      message.success(`Collection ${params.name} 创建成功`)
      setCreateModalVisible(false)
      form.resetFields()
      loadData()
    } catch (error: any) {
      if (error?.errorFields) return // form validation error
      const msg = error?.response?.data?.message || error?.message || '创建失败'
      message.error(msg)
    }
  }

  const handleDelete = async (name: string) => {
    try {
      await deleteCollection(name)
      message.success(`Collection ${name} 已删除`)
      loadData()
    } catch (error: any) {
      const msg = error?.response?.data?.message || error?.message || '删除失败'
      message.error(msg)
    }
  }

  const columns = [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => (
        <Space>
          <DatabaseOutlined />
          <Text strong>{name}</Text>
        </Space>
      ),
    },
    {
      title: '副本策略',
      dataIndex: 'replication',
      key: 'replication',
      render: (r: string) => <Tag color="blue">{r || '-'}</Tag>,
    },
    {
      title: '磁盘类型',
      dataIndex: 'disk_type',
      key: 'disk_type',
      render: (d: string) => <Tag>{d || '-'}</Tag>,
    },
    {
      title: 'TTL',
      dataIndex: 'ttl',
      key: 'ttl',
      render: (t: string) => (t ? <Tag color="orange">{t}</Tag> : <Text type="secondary">-</Text>),
    },
    {
      title: 'Volume 数',
      dataIndex: 'volume_count',
      key: 'volume_count',
      render: (v: number) => formatNumber(v),
    },
    {
      title: 'Volume 上限',
      dataIndex: 'max_volume_count',
      key: 'max_volume_count',
      render: (v: number) => (v > 0 ? formatNumber(v) : <Text type="secondary">无限制</Text>),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: formatTimestamp,
    },
    {
      title: '操作',
      key: 'action',
      render: (_: any, record: CollectionInfo) => (
        <Space>
          <Tooltip title="详情">
            <Button
              type="text"
              icon={<InfoCircleOutlined />}
              onClick={() => setDetailRecord(record)}
            />
          </Tooltip>
          <Popconfirm
            title="确认删除该 Collection？"
            description="删除后该 Collection 下的 Volume 仍保留，但不再受策略约束。"
            onConfirm={() => handleDelete(record.name)}
            okText="删除"
            cancelText="取消"
            okButtonProps={{ danger: true }}
          >
            <Button type="text" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  return (
    <div>
      <Card
        title="Collection 管理"
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={loadData} loading={loading}>
              刷新
            </Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateModalVisible(true)}>
              新建 Collection
            </Button>
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={collections}
          rowKey="name"
          loading={loading}
          pagination={{ pageSize: 20, showSizeChanger: true }}
        />
      </Card>

      <Modal
        title="新建 Collection"
        open={createModalVisible}
        onOk={handleCreate}
        onCancel={() => {
          setCreateModalVisible(false)
          form.resetFields()
        }}
        okText="创建"
        cancelText="取消"
      >
        <Form form={form} layout="vertical" initialValues={{ replication: '001', disk_type: 'hdd' }}>
          <Form.Item
            name="name"
            label="Collection 名称"
            rules={[{ required: true, message: '请输入名称' }]}
          >
            <Input placeholder="例如 ml-cache" />
          </Form.Item>
          <Form.Item name="replication" label="副本策略" tooltip="001=单副本, 002=两副本等">
            <Input placeholder="001" />
          </Form.Item>
          <Form.Item name="disk_type" label="磁盘类型">
            <Input placeholder="hdd" />
          </Form.Item>
          <Form.Item name="ttl" label="TTL" tooltip="可选，时间周期字符串">
            <Input placeholder="" />
          </Form.Item>
          <Form.Item name="max_volume_count" label="Volume 上限" tooltip="0 表示无限制">
            <InputNumber min={0} style={{ width: '100%' }} placeholder="0" />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="Collection 详情"
        open={!!detailRecord}
        onCancel={() => setDetailRecord(null)}
        footer={<Button onClick={() => setDetailRecord(null)}>关闭</Button>}
      >
        {detailRecord && (
          <Descriptions column={1} bordered size="small">
            <Descriptions.Item label="名称">{detailRecord.name}</Descriptions.Item>
            <Descriptions.Item label="副本策略">{detailRecord.replication || '-'}</Descriptions.Item>
            <Descriptions.Item label="磁盘类型">{detailRecord.disk_type || '-'}</Descriptions.Item>
            <Descriptions.Item label="TTL">{detailRecord.ttl || '-'}</Descriptions.Item>
            <Descriptions.Item label="Volume 数">
              {formatNumber(detailRecord.volume_count)}
            </Descriptions.Item>
            <Descriptions.Item label="Volume 上限">
              {detailRecord.max_volume_count > 0
                ? formatNumber(detailRecord.max_volume_count)
                : '无限制'}
            </Descriptions.Item>
            <Descriptions.Item label="创建时间">
              {formatTimestamp(detailRecord.created_at)}
            </Descriptions.Item>
            <Descriptions.Item label="修改时间">
              {formatTimestamp(detailRecord.modified_at)}
            </Descriptions.Item>
          </Descriptions>
        )}
      </Modal>
    </div>
  )
}

export default Collections
