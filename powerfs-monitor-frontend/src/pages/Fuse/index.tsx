import { useState, useEffect } from 'react'
import { Card, Table, Tag, Button, Modal, Form, Input, Space, Popconfirm, message } from 'antd'
import {
  FolderOutlined,
  PlusOutlined,
  DeleteOutlined,
} from '@ant-design/icons'
import type { FuseMount } from '@/types'
import { getFuseMounts, createFuseMount, deleteFuseMount } from '@/services/api'

function Fuse() {
  const [mounts, setMounts] = useState<FuseMount[]>([])
  const [createModalVisible, setCreateModalVisible] = useState(false)
  const [form] = Form.useForm()

  useEffect(() => {
    loadMounts()
  }, [])

  const loadMounts = async () => {
    try {
      const mountList = await getFuseMounts()
      setMounts(mountList)
    } catch (error) {
      console.error('Failed to load FUSE mounts:', error)
      message.error('加载FUSE挂载列表失败')
    }
  }

  const handleCreateMount = async () => {
    try {
      const values = await form.validateFields()
      await createFuseMount({
        mount_point: values.mount_point,
        collection: values.collection,
        replication: values.replication,
        master: values.master,
        threads: values.threads,
      })
      setCreateModalVisible(false)
      form.resetFields()
      loadMounts()
      message.success('FUSE挂载创建成功')
    } catch (error) {
      console.error('Failed to create FUSE mount:', error)
      message.error('创建FUSE挂载失败')
    }
  }

  const handleDeleteMount = async (id: string) => {
    try {
      await deleteFuseMount(id)
      loadMounts()
      message.success('FUSE挂载已卸载')
    } catch (error) {
      console.error('Failed to delete FUSE mount:', error)
      message.error('卸载FUSE挂载失败')
    }
  }

  const columns = [
    {
      title: '客户端ID',
      dataIndex: 'id',
      key: 'id',
      width: 100,
      render: (id: string) => id.slice(0, 8) + '...',
    },
    {
      title: '主机',
      dataIndex: 'host',
      key: 'host',
      width: 120,
    },
    {
      title: '挂载点',
      dataIndex: 'mount_point',
      key: 'mount_point',
      render: (path: string) => (
        <span>
          <FolderOutlined style={{ marginRight: 8, color: '#1890ff' }} />
          {path}
        </span>
      ),
    },
    {
      title: 'Collection',
      dataIndex: 'collection',
      key: 'collection',
    },
    {
      title: '副本策略',
      dataIndex: 'replication',
      key: 'replication',
    },
    {
      title: '脏Chunks',
      dataIndex: 'dirty_chunks',
      key: 'dirty_chunks',
      width: 80,
      render: (dirty: number | undefined) => (
        <Tag color={dirty && dirty > 0 ? 'orange' : 'green'}>
          {dirty ?? 0}
        </Tag>
      ),
    },
    {
      title: '脏数据',
      dataIndex: 'dirty_bytes',
      key: 'dirty_bytes',
      width: 100,
      render: (bytes: number | undefined) => {
        if (!bytes) return '0 B'
        if (bytes < 1024) return `${bytes} B`
        if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
        if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
        return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
      },
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={status === 'mounted' ? 'green' : status === 'unmounted' ? 'gray' : 'red'}>
          {status === 'mounted' ? '已挂载' : status === 'unmounted' ? '已卸载' : '异常'}
        </Tag>
      ),
    },
    {
      title: '挂载时间',
      dataIndex: 'mounted_at',
      key: 'mounted_at',
      render: (date: string) => date ? new Date(date).toLocaleString() : '-',
    },
    {
      title: '最后心跳',
      dataIndex: 'last_heartbeat',
      key: 'last_heartbeat',
      render: (date: string) => date ? new Date(date).toLocaleString() : '-',
    },
    {
      title: '进程ID',
      dataIndex: 'pid',
      key: 'pid',
      width: 70,
      render: (pid: number | undefined) => pid ?? '-',
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: unknown, record: FuseMount) => (
        <Space>
          <Popconfirm
            title={`确定卸载 "${record.mount_point}" 吗？`}
            onConfirm={() => handleDeleteMount(record.id)}
            okText="确定"
            cancelText="取消"
          >
            <Button size="small" danger>
              <DeleteOutlined /> 卸载
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ]

  return (
    <div>
      <Card
        title="FUSE 挂载管理"
        style={{ borderRadius: 12 }}
        bodyStyle={{ padding: '20px' }}
        extra={
          <Button type="primary" onClick={() => setCreateModalVisible(true)}>
            <PlusOutlined /> 新建挂载
          </Button>
        }
      >
        <Table
          columns={columns}
          dataSource={mounts}
          rowKey="id"
          pagination={{ pageSize: 10 }}
          size="small"
        />
      </Card>

      <Modal
        title="新建 FUSE 挂载"
        visible={createModalVisible}
        onCancel={() => { setCreateModalVisible(false); form.resetFields(); }}
        footer={null}
      >
        <Form form={form} layout="vertical" onFinish={handleCreateMount}>
          <Form.Item
            name="mount_point"
            label="挂载点路径"
            rules={[{ required: true, message: '请输入挂载点路径' }]}
          >
            <Input placeholder="/mnt/powerfs" />
          </Form.Item>
          <Form.Item
            name="collection"
            label="Collection名称"
            rules={[{ required: true, message: '请输入Collection名称' }]}
          >
            <Input placeholder="default" />
          </Form.Item>
          <Form.Item
            name="replication"
            label="副本策略"
            rules={[{ required: true, message: '请输入副本策略' }]}
          >
            <Input placeholder="000" />
          </Form.Item>
          <Form.Item
            name="master"
            label="Master节点地址"
            rules={[{ required: true, message: '请输入Master节点地址' }]}
          >
            <Input placeholder="localhost:9333" />
          </Form.Item>
          <Form.Item
            name="threads"
            label="工作线程数"
            rules={[{ required: true, message: '请输入工作线程数' }]}
          >
            <Input type="number" placeholder="8" />
          </Form.Item>
          <Form.Item>
            <Space>
              <Button onClick={() => { setCreateModalVisible(false); form.resetFields(); }}>取消</Button>
              <Button type="primary" htmlType="submit">创建</Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

export default Fuse