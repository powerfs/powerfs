import React, { useEffect, useState, useMemo } from 'react'
import { Card, Table, Tag, Button, Modal, Space, Progress, message, Tabs } from 'antd'
import {
  SaveOutlined,
  DeleteOutlined,
  EyeOutlined,
  PlusOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  LeftCircleOutlined,
  DatabaseOutlined,
  CloudServerOutlined,
  HddOutlined,
  ExclamationCircleOutlined,
} from '@ant-design/icons'
import type { NodeInfo, StorageDevice } from '@/types'
import { getNodes, deleteNode, getDevices } from '@/services/api'
import { formatBytes, formatPercent, formatUptime, formatTime } from '@/utils/format'

function Nodes() {
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [selectedNode, setSelectedNode] = useState<NodeInfo | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [nodeDevices, setNodeDevices] = useState<StorageDevice[]>([])

  useEffect(() => {
    loadNodes()
    const interval = setInterval(loadNodes, 10000)
    return () => clearInterval(interval)
  }, [])

  const loadNodes = async () => {
    const data = await getNodes()
    setNodes(data)
  }

  const loadNodeDevices = async (nodeId: string) => {
    const data = await getDevices(nodeId)
    setNodeDevices(data)
  }

  const handleViewDetail = (node: NodeInfo) => {
    setSelectedNode(node)
    setShowDetail(true)
    loadNodeDevices(node.id)
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
        const config: Record<string, { color: string; icon: React.ReactNode; text: string }> = {
          online: { color: 'green', icon: <CheckCircleOutlined />, text: '在线' },
          offline: { color: 'red', icon: <CloseCircleOutlined />, text: '离线' },
          warning: { color: 'orange', icon: <LeftCircleOutlined />, text: '告警' },
          healthy: { color: 'green', icon: <CheckCircleOutlined />, text: '健康' },
        }
        const { color, icon, text } = config[status] || { color: 'default', icon: null, text: status }
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

  const masterNodes = useMemo(() => nodes.filter(n => n.node_type === 'master'), [nodes])
  const volumeNodes = useMemo(() => nodes.filter(n => n.node_type === 'volume'), [nodes])

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
        <Tabs
          items={[
            {
              key: 'master',
              label: (
                <span>
                  <CloudServerOutlined /> Master节点 ({masterNodes.length})
                </span>
              ),
              children: (
                <Table
                  columns={columns}
                  dataSource={masterNodes}
                  rowKey="id"
                  pagination={{ pageSize: 10 }}
                  scroll={{ x: 1000 }}
                  locale={{ emptyText: '暂无Master节点' }}
                />
              ),
            },
            {
              key: 'volume',
              label: (
                <span>
                  <DatabaseOutlined /> Volume节点 ({volumeNodes.length})
                </span>
              ),
              children: (
                <Table
                  columns={columns}
                  dataSource={volumeNodes}
                  rowKey="id"
                  pagination={{ pageSize: 10 }}
                  scroll={{ x: 1000 }}
                  locale={{ emptyText: '暂无Volume节点' }}
                />
              ),
            },
          ]}
        />
      </Card>

      <Modal
        title="节点详情"
        open={showDetail}
        onCancel={() => setShowDetail(false)}
        footer={null}
        width={900}
        destroyOnClose
      >
        {selectedNode && (
          <Tabs
            items={[
              {
                key: 'basic',
                label: '基本信息',
                children: (
                  <Space direction="vertical" style={{ width: '100%', gap: 20, paddingTop: 8 }}>
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
                ),
              },
              {
                key: 'devices',
                label: (
                  <span>
                    <HddOutlined /> 存储设备 ({nodeDevices.length})
                  </span>
                ),
                children: (
                  <Table
                    columns={[
                      {
                        title: '设备ID',
                        dataIndex: 'device_id',
                        key: 'device_id',
                        width: 100,
                      },
                      {
                        title: '类型',
                        dataIndex: 'device_type',
                        key: 'device_type',
                        width: 100,
                        render: (type: string) => {
                          const typeMap: Record<string, string> = {
                            local_file: '本地文件',
                            spdk: 'SPDK',
                            nvmeof: 'NVMe-oF',
                          }
                          return typeMap[type] || type
                        },
                      },
                      {
                        title: '状态',
                        dataIndex: 'status',
                        key: 'status',
                        width: 90,
                        render: (status: string) => {
                          const statusConfig: Record<string, { color: string; text: string }> = {
                            online: { color: 'green', text: '在线' },
                            offline: { color: 'red', text: '离线' },
                            excluded: { color: 'orange', text: '已排除' },
                            draining: { color: 'blue', text: '排空中' },
                            faulty: { color: 'red', text: '故障' },
                          }
                          const cfg = statusConfig[status] || { color: 'default', text: status }
                          return <Tag color={cfg.color}>{cfg.text}</Tag>
                        },
                      },
                      {
                        title: '健康度',
                        dataIndex: 'health',
                        key: 'health',
                        width: 90,
                        render: (health?: string) => {
                          if (!health) return '-'
                          const healthConfig: Record<string, { color: string; icon: React.ReactNode; text: string }> = {
                            healthy: { color: 'green', icon: <CheckCircleOutlined />, text: '健康' },
                            warning: { color: 'orange', icon: <ExclamationCircleOutlined />, text: '警告' },
                            critical: { color: 'red', icon: <CloseCircleOutlined />, text: '严重' },
                          }
                          const cfg = healthConfig[health] || { color: 'default', icon: null, text: health }
                          return (
                            <Tag color={cfg.color}>
                              {cfg.icon} {cfg.text}
                            </Tag>
                          )
                        },
                      },
                      {
                        title: '总容量',
                        dataIndex: 'total_capacity',
                        key: 'total_capacity',
                        width: 100,
                        render: (val: number) => formatBytes(val),
                      },
                      {
                        title: '已用空间',
                        key: 'used',
                        width: 160,
                        render: (_: unknown, record: StorageDevice) => {
                          const percent = record.total_capacity > 0
                            ? (record.used_space / record.total_capacity) * 100
                            : 0
                          return (
                            <div>
                              <Progress
                                percent={parseFloat(percent.toFixed(1))}
                                size="small"
                                strokeColor={percent > 80 ? '#f5222d' : percent > 60 ? '#faad14' : '#52c41a'}
                                showInfo={false}
                              />
                              <span style={{ marginLeft: 8, fontSize: 12, color: '#8c8c8c' }}>
                                {formatBytes(record.used_space)}
                              </span>
                            </div>
                          )
                        },
                      },
                      {
                        title: 'Volume数',
                        dataIndex: 'volume_count',
                        key: 'volume_count',
                        width: 80,
                      },
                      {
                        title: '最后检查',
                        dataIndex: 'last_check',
                        key: 'last_check',
                        width: 150,
                        render: (val?: string) => val ? formatTime(val) : '-',
                      },
                    ]}
                    dataSource={nodeDevices}
                    rowKey="device_id"
                    pagination={{ pageSize: 5 }}
                    scroll={{ x: 800 }}
                    size="small"
                    locale={{ emptyText: '暂无存储设备' }}
                  />
                ),
              },
            ]}
          />
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