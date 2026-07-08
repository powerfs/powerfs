import { useEffect, useState } from 'react'
import {
  Card,
  Table,
  Button,
  Modal,
  Space,
  message,
  Popconfirm,
  Tag,
  Typography,
  Input,
} from 'antd'
import { PlusOutlined, DeleteOutlined, CopyOutlined, KeyOutlined } from '@ant-design/icons'
import {
  listAccessKeys,
  createAccessKey,
  deleteAccessKey,
  type S3AccessKeyInfo,
  type CreatedAccessKey,
} from '@/services/s3keys'

const { Text, Paragraph } = Typography

function AccessKeys() {
  const [keys, setKeys] = useState<S3AccessKeyInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [createdModal, setCreatedModal] = useState<CreatedAccessKey | null>(null)

  const fetchKeys = async () => {
    setLoading(true)
    try {
      const data = await listAccessKeys()
      setKeys(data)
    } catch (e: any) {
      message.error(e?.message || '加载密钥列表失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchKeys()
  }, [])

  const handleCreate = async () => {
    try {
      const created = await createAccessKey()
      setCreatedModal(created)
      fetchKeys()
    } catch (e: any) {
      message.error(e?.message || '创建密钥失败')
    }
  }

  const handleDelete = async (id: string) => {
    try {
      await deleteAccessKey(id)
      message.success('删除成功')
      fetchKeys()
    } catch (e: any) {
      message.error(e?.message || '删除失败')
    }
  }

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
    message.success('已复制到剪贴板')
  }

  const columns = [
    {
      title: 'AccessKey',
      dataIndex: 'access_key',
      key: 'access_key',
      render: (ak: string) => (
        <Space>
          <KeyOutlined />
          <code style={{ fontFamily: 'monospace' }}>{ak}</code>
          <Button
            type="text"
            size="small"
            icon={<CopyOutlined />}
            onClick={() => copyToClipboard(ak)}
          />
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={status === 'active' ? 'green' : 'default'}>
          {status === 'active' ? '启用' : '禁用'}
        </Tag>
      ),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (t: string) => new Date(t).toLocaleString('zh-CN'),
    },
    {
      title: '最后使用',
      dataIndex: 'last_used_at',
      key: 'last_used_at',
      render: (t: string | null) => (t ? new Date(t).toLocaleString('zh-CN') : '从未使用'),
    },
    {
      title: '操作',
      key: 'action',
      render: (_: any, record: S3AccessKeyInfo) => (
        <Popconfirm
          title="确认删除该密钥？删除后使用该密钥的应用将无法访问 S3。"
          onConfirm={() => handleDelete(record.id)}
          okText="删除"
          cancelText="取消"
        >
          <Button type="link" size="small" danger icon={<DeleteOutlined />}>
            删除
          </Button>
        </Popconfirm>
      ),
    },
  ]

  return (
    <Card
      title="S3 AccessKey 管理"
      extra={
        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
          创建密钥
        </Button>
      }
    >
      <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
        管理你自己的 S3 AccessKey。每个 AccessKey 对应一对 access_key/secret_key，用于 S3 API
        认证。secret_key 仅在创建时显示一次，请妥善保存。
      </Text>
      <Table
        columns={columns}
        dataSource={keys}
        rowKey="id"
        loading={loading}
        pagination={false}
      />

      <Modal
        title="新创建的 AccessKey"
        open={!!createdModal}
        onOk={() => setCreatedModal(null)}
        onCancel={() => setCreatedModal(null)}
        okText="我已保存"
        cancelText="关闭"
        width={600}
        closable={false}
        maskClosable={false}
      >
        {createdModal && (
          <Space direction="vertical" style={{ width: '100%' }} size="middle">
            <div>
              <Text strong>AccessKey:</Text>
              <Input.Group compact>
                <Input
                  style={{ width: 'calc(100% - 32px)', fontFamily: 'monospace' }}
                  value={createdModal.access_key}
                  readOnly
                />
                <Button
                  icon={<CopyOutlined />}
                  onClick={() => copyToClipboard(createdModal.access_key)}
                />
              </Input.Group>
            </div>
            <div>
              <Text strong>SecretKey:</Text>
              <Paragraph type="warning" style={{ margin: '4px 0' }}>
                SecretKey 仅显示此一次，离开此对话框后将无法再次查看。请立即复制保存。
              </Paragraph>
              <Input.Group compact>
                <Input
                  style={{ width: 'calc(100% - 32px)', fontFamily: 'monospace' }}
                  value={createdModal.secret_key}
                  readOnly
                />
                <Button
                  icon={<CopyOutlined />}
                  onClick={() => copyToClipboard(createdModal.secret_key)}
                />
              </Input.Group>
            </div>
          </Space>
        )}
      </Modal>
    </Card>
  )
}

export default AccessKeys
