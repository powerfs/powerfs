import { useEffect, useState } from 'react'
import {
  Card,
  Table,
  Button,
  Modal,
  Form,
  Input,
  Space,
  message,
  Popconfirm,
  Tag,
  Typography,
} from 'antd'
import { PlusOutlined, EditOutlined, DeleteOutlined, KeyOutlined } from '@ant-design/icons'
import {
  listRoles,
  createRole,
  updateRole,
  deleteRole,
  type Role,
  type CreateRoleRequest,
} from '@/services/roles'

const { Text } = Typography

// 常用权限预设
const PERMISSION_PRESETS = [
  { label: 'S3 读', value: 's3:read' },
  { label: 'S3 写', value: 's3:write' },
  { label: 'S3 删除', value: 's3:delete' },
  { label: 'KV 读', value: 'kv:read' },
  { label: 'KV 写', value: 'kv:write' },
  { label: '告警读', value: 'alert:read' },
  { label: '用户管理', value: 'user:read' },
  { label: '全部权限', value: '*' },
]

function Roles() {
  const [roles, setRoles] = useState<Role[]>([])
  const [loading, setLoading] = useState(false)
  const [modalOpen, setModalOpen] = useState(false)
  const [editingRole, setEditingRole] = useState<Role | null>(null)
  const [form] = Form.useForm()

  const fetchRoles = async () => {
    setLoading(true)
    try {
      const data = await listRoles()
      setRoles(data)
    } catch (e: any) {
      message.error(e?.message || '加载角色列表失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchRoles()
  }, [])

  const handleCreate = () => {
    setEditingRole(null)
    form.resetFields()
    form.setFieldsValue({ permissions: [] })
    setModalOpen(true)
  }

  const handleEdit = (role: Role) => {
    setEditingRole(role)
    form.setFieldsValue({
      name: role.name,
      description: role.description,
      permissions: role.permissions,
    })
    setModalOpen(true)
  }

  const handleDelete = async (id: string) => {
    try {
      await deleteRole(id)
      message.success('删除成功')
      fetchRoles()
    } catch (e: any) {
      message.error(e?.message || '删除失败')
    }
  }

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields()
      if (editingRole) {
        await updateRole(editingRole.id, values)
        message.success('更新成功')
      } else {
        const req: CreateRoleRequest = {
          name: values.name,
          description: values.description || '',
          permissions: values.permissions || [],
        }
        await createRole(req)
        message.success('创建成功')
      }
      setModalOpen(false)
      fetchRoles()
    } catch (e: any) {
      if (e?.errorFields) return // 表单校验错误
      message.error(e?.message || '操作失败')
    }
  }

  const columns = [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string, record: Role) => (
        <Space>
          <KeyOutlined />
          <span style={{ fontWeight: 500 }}>{name}</span>
          {record.permissions.includes('*') && <Tag color="red">超级权限</Tag>}
        </Space>
      ),
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '权限',
      dataIndex: 'permissions',
      key: 'permissions',
      render: (perms: string[]) => (
        <Space wrap>
          {perms.map((p) => (
            <Tag key={p} color={p === '*' ? 'red' : 'blue'}>
              {p}
            </Tag>
          ))}
        </Space>
      ),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (t: string) => new Date(t).toLocaleString('zh-CN'),
    },
    {
      title: '操作',
      key: 'action',
      render: (_: any, record: Role) => (
        <Space>
          <Button
            type="link"
            size="small"
            icon={<EditOutlined />}
            onClick={() => handleEdit(record)}
          >
            编辑
          </Button>
          <Popconfirm
            title="确认删除该角色？"
            onConfirm={() => handleDelete(record.id)}
            okText="删除"
            cancelText="取消"
          >
            <Button type="link" size="small" danger icon={<DeleteOutlined />}>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ]

  return (
    <Card
      title="角色管理"
      extra={
        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
          新建角色
        </Button>
      }
    >
      <Table
        columns={columns}
        dataSource={roles}
        rowKey="id"
        loading={loading}
        pagination={false}
      />
      <Modal
        title={editingRole ? '编辑角色' : '新建角色'}
        open={modalOpen}
        onOk={handleSubmit}
        onCancel={() => setModalOpen(false)}
        okText="保存"
        cancelText="取消"
        width={600}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="name"
            label="角色名称"
            rules={[{ required: true, message: '请输入角色名称' }]}
          >
            <Input placeholder="如 viewer、operator" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input.TextArea rows={2} placeholder="角色用途说明" />
          </Form.Item>
          <Form.Item name="permissions" label="权限列表">
            <Input.TextArea
              rows={6}
              placeholder={'每行一个权限，如：\ns3:read\ns3:write\nkv:read'}
            />
          </Form.Item>
          <Text type="secondary" style={{ fontSize: 12 }}>
            可用权限预设：
            {PERMISSION_PRESETS.map((p) => p.value).join('、')}
            。使用 &quot;*&quot; 表示全部权限，&quot;resource:*&quot; 表示该资源的全部操作。
          </Text>
        </Form>
      </Modal>
    </Card>
  )
}

export default Roles
