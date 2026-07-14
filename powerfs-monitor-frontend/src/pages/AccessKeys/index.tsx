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
  Tabs,
} from 'antd'
import { PlusOutlined, DeleteOutlined, CopyOutlined, KeyOutlined } from '@ant-design/icons'
import {
  listAccessKeys,
  createAccessKey,
  deleteAccessKey,
  type S3AccessKeyInfo,
  type CreatedAccessKey,
} from '@/services/s3keys'
import { listKVKeys, createKVKey, deleteKVKey } from '@/services/api'
import type { KVAccessKey } from '@/types'

const { Text, Paragraph } = Typography

type CreatedMonitorKey = {
  id: string
  user_id: string
  access_key: string
  api_key: string
  status: string
  created_at: string
}

function AccessKeys() {
  const [keys, setKeys] = useState<S3AccessKeyInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [createdModal, setCreatedModal] = useState<CreatedAccessKey | null>(null)

  // Monitor API Key 状态
  const [monitorKeys, setMonitorKeys] = useState<KVAccessKey[]>([])
  const [monitorLoading, setMonitorLoading] = useState(false)
  const [createdMonitorModal, setCreatedMonitorModal] = useState<CreatedMonitorKey | null>(null)

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

  const fetchMonitorKeys = async () => {
    setMonitorLoading(true)
    try {
      const data = await listKVKeys()
      setMonitorKeys(data)
    } catch (e: any) {
      message.error(e?.message || '加载 API Key 列表失败')
    } finally {
      setMonitorLoading(false)
    }
  }

  useEffect(() => {
    fetchKeys()
    fetchMonitorKeys()
  }, [])

  const handleCreate = async () => {
    try {
      const created = await createAccessKey()
      setCreatedModal(created)
      fetchKeys()
    } catch (e: any) {
      message.error(e?.message || '创建失败')
    }
  }

  const handleCreateMonitorKey = async () => {
    try {
      const created = await createKVKey()
      setCreatedMonitorModal(created)
      fetchMonitorKeys()
    } catch (e: any) {
      message.error(e?.message || '创建失败')
    }
  }

  const handleDelete = async (id: string) => {
    try {
      await deleteAccessKey(id)
      message.success('已删除')
      fetchKeys()
    } catch (e: any) {
      message.error(e?.message || '删除失败')
    }
  }

  const handleDeleteMonitorKey = async (id: string) => {
    try {
      await deleteKVKey(id)
      message.success('已吊销')
      fetchMonitorKeys()
    } catch (e: any) {
      message.error(e?.message || '删除失败')
    }
  }

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
    message.success('已复制到剪贴板')
  }

  // S3 AccessKey 表格列
  const s3Columns = [
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
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (t: string) => (t ? new Date(t).toLocaleString() : '-'),
    },
    {
      title: '操作',
      key: 'actions',
      width: 100,
      render: (_: unknown, record: S3AccessKeyInfo) => (
        <Popconfirm
          title={`确定删除 AccessKey "${record.access_key}" 吗？`}
          onConfirm={() => handleDelete(record.id)}
          okText="确定"
          cancelText="取消"
        >
          <Button type="link" size="small" danger icon={<DeleteOutlined />}>
            删除
          </Button>
        </Popconfirm>
      ),
    },
  ]

  // Monitor API Key 表格列
  const monitorColumns = [
    {
      title: 'AccessKey',
      dataIndex: 'access_key',
      key: 'access_key',
      render: (ak: string) => (
        <Space>
          <KeyOutlined />
          <code style={{ fontFamily: 'monospace' }}>{ak}</code>
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => (
        <Tag color={status === 'active' ? 'green' : 'default'}>
          {status === 'active' ? '启用' : '已禁用'}
        </Tag>
      ),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (t: string) => (t ? new Date(t).toLocaleString() : '-'),
    },
    {
      title: '最后使用',
      dataIndex: 'last_used_at',
      key: 'last_used_at',
      render: (t: string | null) => (t ? new Date(t).toLocaleString() : '-'),
    },
    {
      title: '操作',
      key: 'actions',
      width: 100,
      render: (_: unknown, record: KVAccessKey) => (
        <Popconfirm
          title={`确定吊销 API Key "${record.access_key}" 吗？`}
          description="吊销后，使用此 Key 的 Python SDK / Agent 将无法访问 API。"
          onConfirm={() => handleDeleteMonitorKey(record.id)}
          okText="确定吊销"
          cancelText="取消"
          okButtonProps={{ danger: true }}
        >
          <Button type="link" size="small" danger icon={<DeleteOutlined />}>
            吊销
          </Button>
        </Popconfirm>
      ),
    },
  ]

  return (
    <Tabs
      defaultActiveKey="s3"
      items={[
        {
          key: 's3',
          label: 'S3 AccessKey',
          children: (
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
                columns={s3Columns}
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
          ),
        },
        {
          key: 'monitor',
          label: 'Monitor API Key',
          children: (
            <Card
              title="Monitor API Key 管理"
              extra={
                <Button
                  type="primary"
                  icon={<PlusOutlined />}
                  onClick={handleCreateMonitorKey}
                >
                  创建 API Key
                </Button>
              }
            >
              <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
                长效 API Key，用于 Python SDK / Agent 访问 Monitor API（冲突管理、FUSE 监控等）。
                格式为 <code>pak_&lt;access_key&gt;_&lt;secret_key&gt;</code>，不会过期，
                直到手动吊销。完整 API Key 仅在创建时显示一次，请妥善保存。
              </Text>
              <Table
                columns={monitorColumns}
                dataSource={monitorKeys}
                rowKey="id"
                loading={monitorLoading}
                pagination={false}
              />

              <Modal
                title="新创建的 Monitor API Key"
                open={!!createdMonitorModal}
                onOk={() => setCreatedMonitorModal(null)}
                onCancel={() => setCreatedMonitorModal(null)}
                okText="我已保存"
                cancelText="关闭"
                width={700}
                closable={false}
                maskClosable={false}
              >
                {createdMonitorModal && (
                  <Space direction="vertical" style={{ width: '100%' }} size="middle">
                    <div>
                      <Text strong>完整 API Key（用于 Python SDK）:</Text>
                      <Paragraph type="warning" style={{ margin: '4px 0' }}>
                        此 Key 仅显示一次，离开后将无法再次查看。请立即复制保存到安全位置。
                      </Paragraph>
                      <Input.Group compact>
                        <Input
                          style={{ width: 'calc(100% - 32px)', fontFamily: 'monospace' }}
                          value={createdMonitorModal.api_key}
                          readOnly
                        />
                        <Button
                          icon={<CopyOutlined />}
                          onClick={() => copyToClipboard(createdMonitorModal.api_key)}
                        />
                      </Input.Group>
                    </div>
                    <div>
                      <Text strong>Python SDK 使用示例:</Text>
                      <pre
                        style={{
                          background: '#f5f5f5',
                          padding: 12,
                          borderRadius: 4,
                          fontSize: 13,
                          margin: '4px 0',
                        }}
                      >
{`from powerfs import PowerFSClient

client = PowerFSClient(
    master_endpoint="http://monitor:8084",
    api_key="${createdMonitorModal.api_key}"
)

# 获取冲突列表
conflicts = client.conflict_manager.get_conflicts()`}
                      </pre>
                    </div>
                  </Space>
                )}
              </Modal>
            </Card>
          ),
        },
      ]}
    />
  )
}

export default AccessKeys
