import { useEffect, useState } from 'react'
import { Card, Table, Tag, Button, Modal, Space, Progress, message } from 'antd'
import {
  SaveOutlined,
  DeleteOutlined,
  EyeOutlined,
  PlusOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  LeftCircleOutlined,
} from '@ant-design/icons'
import type { NodeInfo } from '@/types'
import { getNodes, deleteNode } from '@/services/api'
import { formatBytes, formatPercent, formatUptime } from '@/utils/format'

function Nodes() {
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [selectedNode, setSelectedNode] = useState<NodeInfo | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)

  useEffect(() => {
    loadNodes()
    const interval = setInterval(loadNodes, 10000)
    return () => clearInterval(interval)
  }, [])

  const loadNodes = async () => {
    const data = await getNodes()
    setNodes(data)
  }

  const handleViewDetail = (node: NodeInfo) => {
    setSelectedNode(node)
    setShowDetail(true)
  }

  const handleDelete = (node: NodeInfo) => {
    setSelectedNode(node)
    setShowDeleteConfirm(true)
  }

  const confirmDelete = async () => {
    if (selectedNode) {
      await deleteNode(selectedNode.id)
      message.success('节点删除成功')
      setShowDeleteConfirm(false)
      loadNodes()
    }
  }

  const columns = [
    {
      title: '节点ID',
      dataIndex: 'id',
      key: 'id',
      width: 120,
    },
    {
      title: '地址',
      dataIndex: 'address',
      key: 'address',
      render: (address: string, record: NodeInfo) => (
        <span>{address}:{record.grpc_port}</span>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => {
        const config = {
          online: { color: 'green', icon: <CheckCircleOutlined />, text: '在线' },
          offline: { color: 'red', icon: <CloseCircleOutlined />, text: '离线' },
          warning: { color: 'orange', icon: <LeftCircleOutlined />, text: '告警' },
        }
        const { color, icon, text } = config[status as keyof typeof config]
        return (
          <Tag color={color}>
            {icon} {text}
          </Tag>
        )
      },
    },
    {
      title: 'CPU',
      key: 'cpu',
      width: 120,
      render: (_: unknown, record: NodeInfo) => (
        <div>
          <Progress
            percent={record.cpu_usage}
            size="small"
            strokeColor={record.cpu_usage > 80 ? '#f5222d' : record.cpu_usage > 60 ? '#faad14' : '#52c41a'}
            showInfo={false}
          />
          <span style={{ marginLeft: 8, fontSize: 12 }}>{formatPercent(record.cpu_usage)}</span>
        </div>
      ),
    },
    {
      title: '内存',
      key: 'mem',
      width: 120,
      render: (_: unknown, record: NodeInfo) => (
        <div>
          <Progress
            percent={record.mem_usage}
            size="small"
            strokeColor={record.mem_usage > 80 ? '#f5222d' : record.mem_usage > 60 ? '#faad14' : '#52c41a'}
            showInfo={false}
          />
          <span style={{ marginLeft: 8, fontSize: 12 }}>{formatPercent(record.mem_usage)}</span>
        </div>
      ),
    },
    {
      title: '磁盘',
      key: 'disk',
      width: 120,
      render: (_: unknown, record: NodeInfo) => (
        <div>
          <Progress
            percent={record.disk_usage}
            size="small"
            strokeColor={record.disk_usage > 80 ? '#f5222d' : record.disk_usage > 60 ? '#faad14' : '#52c41a'}
            showInfo={false}
          />
          <span style={{ marginLeft: 8, fontSize: 12 }}>{formatPercent(record.disk_usage)}</span>
        </div>
      ),
    },
    {
      title: 'Volume数',
      dataIndex: 'volume_count',
      key: 'volume_count',
      width: 80,
    },
    {
      title: '运行时间',
      dataIndex: 'uptime',
      key: 'uptime',
      width: 150,
      render: (uptime: number) => formatUptime(uptime),
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      render: (_: unknown, record: NodeInfo) => (
        <Space>
          <Button
            type="text"
            icon={<EyeOutlined />}
            onClick={() => handleViewDetail(record)}
          >
            详情
          </Button>
          <Button
            type="text"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDelete(record)}
          >
            删除
          </Button>
        </Space>
      ),
    },
  ]

  return (
    <div>
      <Card
        title="节点管理"
        style={{ borderRadius: 12, marginBottom: 16 }}
        extra={
          <Button type="primary" icon={<PlusOutlined />}>
            添加节点
          </Button>
        }
      >
        <Table
          columns={columns}
          dataSource={nodes}
          rowKey="id"
          pagination={{ pageSize: 10 }}
          scroll={{ x: 1000 }}
        />
      </Card>

      <Modal
        title="节点详情"
        open={showDetail}
        onCancel={() => setShowDetail(false)}
        footer={null}
        width={600}
      >
        {selectedNode && (
          <Space direction="vertical" style={{ width: '100%', gap: 20 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ background: '#e6f7ff', padding: 12, borderRadius: 12 }}>
                <SaveOutlined style={{ fontSize: 32, color: '#1890ff' }} />
              </div>
              <div>
                <h3 style={{ margin: 0 }}>{selectedNode.id}</h3>
                <p style={{ margin: '4px 0', color: '#8c8c8c' }}>
                  {selectedNode.address}:{selectedNode.grpc_port}
                </p>
              </div>
            </div>

            <div>
              <h4 style={{ margin: '0 0 12px' }}>资源使用</h4>
              <Space direction="vertical" style={{ width: '100%', gap: 12 }}>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ color: '#8c8c8c' }}>CPU使用率</span>
                    <span>{formatPercent(selectedNode.cpu_usage)}</span>
                  </div>
                  <Progress
                    percent={selectedNode.cpu_usage}
                    strokeColor={selectedNode.cpu_usage > 80 ? '#f5222d' : selectedNode.cpu_usage > 60 ? '#faad14' : '#52c41a'}
                  />
                </div>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ color: '#8c8c8c' }}>内存使用率</span>
                    <span>{formatPercent(selectedNode.mem_usage)}</span>
                  </div>
                  <Progress
                    percent={selectedNode.mem_usage}
                    strokeColor={selectedNode.mem_usage > 80 ? '#f5222d' : selectedNode.mem_usage > 60 ? '#faad14' : '#52c41a'}
                  />
                </div>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ color: '#8c8c8c' }}>磁盘使用率</span>
                    <span>{formatPercent(selectedNode.disk_usage)}</span>
                  </div>
                  <Progress
                    percent={selectedNode.disk_usage}
                    strokeColor={selectedNode.disk_usage > 80 ? '#f5222d' : selectedNode.disk_usage > 60 ? '#faad14' : '#52c41a'}
                  />
                </div>
              </Space>
            </div>

            <div>
              <h4 style={{ margin: '0 0 12px' }}>网络IO</h4>
              <div style={{ display: 'flex', gap: 24 }}>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>接收</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{formatBytes(selectedNode.network_rx)}</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>发送</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{formatBytes(selectedNode.network_tx)}</p>
                </div>
              </div>
            </div>

            <div>
              <h4 style={{ margin: '0 0 12px' }}>状态信息</h4>
              <div style={{ display: 'flex', gap: 24 }}>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>状态</span>
                  <Tag color={selectedNode.status === 'online' ? 'green' : selectedNode.status === 'warning' ? 'orange' : 'red'}>
                    {selectedNode.status === 'online' ? '在线' : selectedNode.status === 'warning' ? '告警' : '离线'}
                  </Tag>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>Volume数量</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedNode.volume_count} 个</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>运行时间</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{formatUptime(selectedNode.uptime)}</p>
                </div>
              </div>
            </div>
          </Space>
        )}
      </Modal>

      <Modal
        title="确认删除"
        open={showDeleteConfirm}
        onCancel={() => setShowDeleteConfirm(false)}
        onOk={confirmDelete}
        okText="确认删除"
        cancelText="取消"
        okButtonProps={{ danger: true }}
      >
        <p>确定要删除节点 <strong>{selectedNode?.id}</strong> 吗？</p>
        <p style={{ color: '#8c8c8c', fontSize: 12 }}>删除前请确保该节点上的Volume已迁移到其他节点。</p>
      </Modal>
    </div>
  )
}

export default Nodes