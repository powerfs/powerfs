import { useEffect, useState } from 'react'
import { Card, Table, Tag, Button, Modal, Space, Progress, Select, message } from 'antd'
import {
  DatabaseOutlined,
  DeleteOutlined,
  EyeOutlined,
  FireOutlined,
} from '@ant-design/icons'
import type { VolumeInfo } from '@/types'
import { getVolumes, deleteVolume } from '@/services/api'
import { formatBytes } from '@/utils/format'

function Volumes() {
  const [volumes, setVolumes] = useState<VolumeInfo[]>([])
  const [selectedVolume, setSelectedVolume] = useState<VolumeInfo | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [showMigrate, setShowMigrate] = useState(false)
  const [filterStatus, setFilterStatus] = useState<string>('')
  const [filterCollection, setFilterCollection] = useState<string>('')

  useEffect(() => {
    loadVolumes()
    const interval = setInterval(loadVolumes, 10000)
    return () => clearInterval(interval)
  }, [])

  const loadVolumes = async () => {
    const data = await getVolumes()
    setVolumes(data)
  }

  const handleViewDetail = (volume: VolumeInfo) => {
    setSelectedVolume(volume)
    setShowDetail(true)
  }

  const handleDelete = (volume: VolumeInfo) => {
    setSelectedVolume(volume)
    setShowDeleteConfirm(true)
  }

  const handleMigrate = (volume: VolumeInfo) => {
    setSelectedVolume(volume)
    setShowMigrate(true)
  }

  const confirmDelete = async () => {
    if (selectedVolume) {
      await deleteVolume(selectedVolume.id)
      message.success('Volume删除成功')
      setShowDeleteConfirm(false)
      loadVolumes()
    }
  }

  const collections = [...new Set(volumes.map(v => v.collection))]
  const filteredVolumes = volumes.filter(v => {
    if (filterStatus && v.status !== filterStatus) return false
    if (filterCollection && v.collection !== filterCollection) return false
    return true
  })

  const columns = [
    {
      title: 'Volume ID',
      dataIndex: 'id',
      key: 'id',
      width: 100,
      render: (id: number) => <strong>{id}</strong>,
    },
    {
      title: '所属节点',
      dataIndex: 'node_id',
      key: 'node_id',
      width: 120,
    },
    {
      title: 'Collection',
      dataIndex: 'collection',
      key: 'collection',
      width: 120,
      render: (collection: string) => (
        <Tag color="blue">{collection}</Tag>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => {
        const config = {
          available: { color: 'green', text: '可用' },
          full: { color: 'red', text: '已满' },
          readonly: { color: 'orange', text: '只读' },
          creating: { color: 'blue', text: '创建中' },
        }
        const { color, text } = config[status as keyof typeof config]
        return <Tag color={color}>{text}</Tag>
      },
    },
    {
      title: '存储使用',
      key: 'storage',
      width: 200,
      render: (_: unknown, record: VolumeInfo) => {
        const percent = (record.used / record.size) * 100
        return (
          <div>
            <Progress
              percent={percent}
              size="small"
              strokeColor={percent > 90 ? '#f5222d' : percent > 70 ? '#faad14' : '#52c41a'}
              showInfo={false}
            />
            <span style={{ marginLeft: 8, fontSize: 12 }}>
              {formatBytes(record.used)} / {formatBytes(record.size)}
            </span>
          </div>
        )
      },
    },
    {
      title: '文件数',
      dataIndex: 'file_count',
      key: 'file_count',
      width: 100,
      render: (count: number) => count.toLocaleString(),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (time: string) => new Date(time).toLocaleString(),
    },
    {
      title: '操作',
      key: 'action',
      width: 180,
      render: (_: unknown, record: VolumeInfo) => (
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
            icon={<FireOutlined />}
            onClick={() => handleMigrate(record)}
            disabled={record.status === 'creating'}
          >
            迁移
          </Button>
          <Button
            type="text"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDelete(record)}
            disabled={record.file_count > 0}
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
        title="Volume管理"
        style={{ borderRadius: 12, marginBottom: 16 }}
        bodyStyle={{ paddingBottom: 16 }}
      >
        <Space style={{ marginBottom: 16 }}>
          <Select
            placeholder="按状态筛选"
            style={{ width: 150 }}
            value={filterStatus || undefined}
            onChange={setFilterStatus}
            options={[
              { value: '', label: '全部' },
              { value: 'available', label: '可用' },
              { value: 'full', label: '已满' },
              { value: 'readonly', label: '只读' },
              { value: 'creating', label: '创建中' },
            ]}
          />
          <Select
            placeholder="按Collection筛选"
            style={{ width: 150 }}
            value={filterCollection || undefined}
            onChange={setFilterCollection}
            options={[
              { value: '', label: '全部' },
              ...collections.map(c => ({ value: c, label: c })),
            ]}
          />
        </Space>
        <Table
          columns={columns}
          dataSource={filteredVolumes}
          rowKey="id"
          pagination={{ pageSize: 10 }}
          scroll={{ x: 1200 }}
        />
      </Card>

      <Modal
        title="Volume详情"
        open={showDetail}
        onCancel={() => setShowDetail(false)}
        footer={null}
        width={500}
      >
        {selectedVolume && (
          <Space direction="vertical" style={{ width: '100%', gap: 20 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ background: '#f6ffed', padding: 12, borderRadius: 12 }}>
                <DatabaseOutlined style={{ fontSize: 32, color: '#52c41a' }} />
              </div>
              <div>
                <h3 style={{ margin: 0 }}>Volume {selectedVolume.id}</h3>
                <p style={{ margin: '4px 0', color: '#8c8c8c' }}>
                  所属节点: {selectedVolume.node_id}
                </p>
              </div>
            </div>

            <div>
              <h4 style={{ margin: '0 0 12px' }}>存储使用</h4>
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span style={{ color: '#8c8c8c' }}>已用空间</span>
                  <span>{formatBytes(selectedVolume.used)}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span style={{ color: '#8c8c8c' }}>总空间</span>
                  <span>{formatBytes(selectedVolume.size)}</span>
                </div>
                <Progress
                  percent={(selectedVolume.used / selectedVolume.size) * 100}
                  strokeColor={(selectedVolume.used / selectedVolume.size) * 100 > 90 ? '#f5222d' : '#52c41a'}
                />
              </div>
            </div>

            <div>
              <h4 style={{ margin: '0 0 12px' }}>基本信息</h4>
              <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>状态</span>
                  <Tag color={selectedVolume.status === 'available' ? 'green' : selectedVolume.status === 'full' ? 'red' : 'orange'}>
                    {selectedVolume.status === 'available' ? '可用' : selectedVolume.status === 'full' ? '已满' : '只读'}
                  </Tag>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>Collection</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedVolume.collection}</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>文件数</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedVolume.file_count.toLocaleString()}</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>创建时间</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{new Date(selectedVolume.created_at).toLocaleString()}</p>
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
        <p>确定要删除 Volume <strong>{selectedVolume?.id}</strong> 吗？</p>
        <p style={{ color: '#8c8c8c', fontSize: 12 }}>只有空的Volume才能删除。</p>
      </Modal>

      <Modal
        title="迁移Volume"
        open={showMigrate}
        onCancel={() => setShowMigrate(false)}
        footer={null}
        width={500}
      >
        {selectedVolume && (
          <Space direction="vertical" style={{ width: '100%', gap: 20 }}>
            <div>
              <p>将 Volume <strong>{selectedVolume.id}</strong> 迁移到:</p>
            </div>
            <Select
              placeholder="选择目标节点"
              style={{ width: '100%' }}
              options={[
                { value: 'node-1', label: 'node-1 (192.168.1.101)' },
                { value: 'node-2', label: 'node-2 (192.168.1.102)' },
                { value: 'node-3', label: 'node-3 (192.168.1.103)' },
              ]}
            />
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
              <Button onClick={() => setShowMigrate(false)}>取消</Button>
              <Button type="primary" onClick={() => {
                message.success('Volume迁移任务已创建')
                setShowMigrate(false)
              }}>
                确认迁移
              </Button>
            </div>
          </Space>
        )}
      </Modal>
    </div>
  )
}

export default Volumes