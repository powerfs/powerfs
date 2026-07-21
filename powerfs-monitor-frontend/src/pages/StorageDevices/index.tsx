import { useEffect, useState, useMemo } from 'react'
import { Card, Table, Tag, Button, Modal, Space, Progress, Select, message, Tabs, Statistic, Row, Col, Typography, Tooltip } from 'antd'
import {
  HddOutlined,
  EyeOutlined,
  DeleteOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  StopOutlined,
  ReloadOutlined,
  PlusOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons'
import type { StorageDevice, DataMigrationTask } from '@/types'
import {
  getDevices,
  excludeDevice,
  restoreDevice,
  drainDevice,
  getMigrationTasks,
  cancelMigration,
  pauseMigration,
  resumeMigration,
} from '@/services/api'
import { formatBytes, formatTime } from '@/utils/format'

const { Text } = Typography

function StorageDevices() {
  const [devices, setDevices] = useState<StorageDevice[]>([])
  const [selectedDevice, setSelectedDevice] = useState<StorageDevice | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [filterType, setFilterType] = useState<string>('')
  const [filterStatus, setFilterStatus] = useState<string>('')
  const [filterNode, setFilterNode] = useState<string>('')
  const [migrationTasks, setMigrationTasks] = useState<DataMigrationTask[]>([])
  const [actionLoading, setActionLoading] = useState<string>('')

  useEffect(() => {
    loadDevices()
    loadMigrations()
    const interval = setInterval(() => {
      loadDevices()
      loadMigrations()
    }, 10000)
    return () => clearInterval(interval)
  }, [])

  const loadDevices = async () => {
    const data = await getDevices()
    setDevices(data)
  }

  const loadMigrations = async () => {
    const data = await getMigrationTasks()
    setMigrationTasks(data)
  }

  const handleViewDetail = (device: StorageDevice) => {
    setSelectedDevice(device)
    setShowDetail(true)
  }

  const handleExclude = async (device: StorageDevice) => {
    setActionLoading(device.device_id)
    try {
      await excludeDevice(device.device_id)
      message.success('设备已排除')
      loadDevices()
    } catch {
      message.error('排除设备失败')
    }
    setActionLoading('')
  }

  const handleRestore = async (device: StorageDevice) => {
    setActionLoading(device.device_id)
    try {
      await restoreDevice(device.device_id)
      message.success('设备已恢复')
      loadDevices()
    } catch {
      message.error('恢复设备失败')
    }
    setActionLoading('')
  }

  const handleDrain = async (device: StorageDevice) => {
    setActionLoading(device.device_id)
    try {
      await drainDevice(device.device_id)
      message.success('设备排空任务已创建')
      loadDevices()
      loadMigrations()
    } catch {
      message.error('创建设备排空任务失败')
    }
    setActionLoading('')
  }

  const handlePauseMigration = async (task: DataMigrationTask) => {
    try {
      await pauseMigration(task.task_id)
      message.success('迁移任务已暂停')
      loadMigrations()
    } catch {
      message.error('暂停迁移任务失败')
    }
  }

  const handleResumeMigration = async (task: DataMigrationTask) => {
    try {
      await resumeMigration(task.task_id)
      message.success('迁移任务已恢复')
      loadMigrations()
    } catch {
      message.error('恢复迁移任务失败')
    }
  }

  const handleCancelMigration = async (task: DataMigrationTask) => {
    try {
      await cancelMigration(task.task_id)
      message.success('迁移任务已取消')
      loadMigrations()
    } catch {
      message.error('取消迁移任务失败')
    }
  }

  const deviceTypes = [...new Set(devices.map(d => d.device_type))]

  const filteredDevices = useMemo(() => {
    return devices.filter(d => {
      if (filterType && d.device_type !== filterType) return false
      if (filterStatus && d.status !== filterStatus) return false
      if (filterNode && d.location.node_id !== filterNode) return false
      return true
    })
  }, [devices, filterType, filterStatus, filterNode])

  const deviceStats = useMemo(() => {
    const total = devices.length
    const online = devices.filter(d => d.status === 'online').length
    const offline = devices.filter(d => d.status === 'offline' || d.status === 'faulty').length
    const totalCapacity = devices.reduce((sum, d) => sum + d.total_capacity, 0)
    return { total, online, offline, totalCapacity }
  }, [devices])

  const deviceTypeMap: Record<string, string> = {
    local_file: '本地文件',
    spdk: 'SPDK',
    nvmeof: 'NVMe-oF',
  }

  const statusConfig: Record<string, { color: string; text: string }> = {
    online: { color: 'green', text: '在线' },
    offline: { color: 'red', text: '离线' },
    excluded: { color: 'orange', text: '已排除' },
    draining: { color: 'blue', text: '排空中' },
    faulty: { color: 'red', text: '故障' },
  }

  const healthConfig: Record<string, { color: string; icon: React.ReactNode; text: string }> = {
    healthy: { color: 'green', icon: <CheckCircleOutlined />, text: '健康' },
    warning: { color: 'orange', icon: <ExclamationCircleOutlined />, text: '警告' },
    critical: { color: 'red', icon: <CloseCircleOutlined />, text: '严重' },
  }

  const migrationStatusConfig: Record<string, { color: string; text: string }> = {
    pending: { color: 'default', text: '等待中' },
    running: { color: 'blue', text: '运行中' },
    paused: { color: 'orange', text: '已暂停' },
    completed: { color: 'green', text: '已完成' },
    failed: { color: 'red', text: '失败' },
    cancelled: { color: 'default', text: '已取消' },
  }

  const migrationTypeMap: Record<string, string> = {
    volume_migration: 'Volume迁移',
    drain_device: '设备排空',
  }

  const deviceColumns = [
    {
      title: '设备ID',
      dataIndex: 'device_id',
      key: 'device_id',
      width: 100,
      render: (id: string) => <strong>{id}</strong>,
    },
    {
      title: '所属节点',
      key: 'node_id',
      width: 100,
      render: (_: unknown, record: StorageDevice) => record.location.node_id,
    },
    {
      title: '类型',
      dataIndex: 'device_type',
      key: 'device_type',
      width: 100,
      render: (type: string) => <Tag color="blue">{deviceTypeMap[type] || type}</Tag>,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => {
        const cfg = statusConfig[status] || { color: 'default', text: status }
        return <Tag color={cfg.color}>{cfg.text}</Tag>
      },
    },
    {
      title: '健康度',
      dataIndex: 'health',
      key: 'health',
      width: 100,
      render: (health?: string) => {
        if (!health) return '-'
        const cfg = healthConfig[health] || { color: 'default', icon: null, text: health }
        return (
          <Tag color={cfg.color}>
            {cfg.icon} {cfg.text}
          </Tag>
        )
      },
    },
    {
      title: '存储使用',
      key: 'storage',
      width: 220,
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
            <span style={{ marginLeft: 8, fontSize: 12 }}>
              {formatBytes(record.used_space)} / {formatBytes(record.total_capacity)}
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
      title: '位置',
      key: 'location',
      width: 150,
      render: (_: unknown, record: StorageDevice) => (
        <span style={{ color: '#8c8c8c', fontSize: 12 }}>
          {record.location.data_center}/{record.location.zone}/{record.location.rack}
        </span>
      ),
    },
    {
      title: '最后检查',
      dataIndex: 'last_check',
      key: 'last_check',
      width: 160,
      render: (val?: string) => val ? formatTime(val) : '-',
    },
    {
      title: '操作',
      key: 'action',
      width: 220,
      render: (_: unknown, record: StorageDevice) => (
        <Space size="small">
          <Button type="text" icon={<EyeOutlined />} onClick={() => handleViewDetail(record)}>
            详情
          </Button>
          {record.status === 'online' && (
            <>
              <Button
                type="text"
                danger
                icon={<DeleteOutlined />}
                loading={actionLoading === record.device_id}
                onClick={() => handleExclude(record)}
              >
                排除
              </Button>
              <Button
                type="text"
                icon={<ReloadOutlined />}
                loading={actionLoading === record.device_id}
                onClick={() => handleDrain(record)}
              >
                排空
              </Button>
            </>
          )}
          {(record.status === 'excluded' || record.status === 'offline') && (
            <Button
              type="text"
              icon={<CheckCircleOutlined />}
              loading={actionLoading === record.device_id}
              onClick={() => handleRestore(record)}
            >
              恢复
            </Button>
          )}
        </Space>
      ),
    },
  ]

  const migrationColumns = [
    {
      title: '任务ID',
      dataIndex: 'task_id',
      key: 'task_id',
      width: 100,
    },
    {
      title: '类型',
      dataIndex: 'migration_type',
      key: 'migration_type',
      width: 100,
      render: (type: string) => migrationTypeMap[type] || type,
    },
    {
      title: '源设备',
      dataIndex: 'source_device_id',
      key: 'source_device_id',
      width: 100,
    },
    {
      title: '目标设备',
      dataIndex: 'target_device_id',
      key: 'target_device_id',
      width: 100,
      render: (val?: string) => val || '-',
    },
    {
      title: '源Volume',
      dataIndex: 'source_volume_id',
      key: 'source_volume_id',
      width: 100,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => {
        const cfg = migrationStatusConfig[status] || { color: 'default', text: status }
        return <Tag color={cfg.color}>{cfg.text}</Tag>
      },
    },
    {
      title: '进度',
      key: 'progress',
      width: 200,
      render: (_: unknown, record: DataMigrationTask) => (
        <div>
          <Progress
            percent={parseFloat(record.progress_percent.toFixed(1))}
            size="small"
            strokeColor={record.status === 'failed' ? '#f5222d' : '#1890ff'}
            showInfo={false}
          />
          <span style={{ marginLeft: 8, fontSize: 12, color: '#8c8c8c' }}>
            {record.progress_percent.toFixed(1)}%
            {record.data_transferred !== undefined && record.total_data !== undefined && (
              <span> ({formatBytes(record.data_transferred)}/{formatBytes(record.total_data)})</span>
            )}
          </span>
        </div>
      ),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 160,
      render: (val: string) => formatTime(val),
    },
    {
      title: '操作',
      key: 'action',
      width: 150,
      render: (_: unknown, record: DataMigrationTask) => (
        <Space size="small">
          {record.status === 'running' && (
            <Button type="text" icon={<PauseCircleOutlined />} onClick={() => handlePauseMigration(record)}>
              暂停
            </Button>
          )}
          {record.status === 'paused' && (
            <Button type="text" icon={<PlayCircleOutlined />} onClick={() => handleResumeMigration(record)}>
              恢复
            </Button>
          )}
          {(record.status === 'running' || record.status === 'paused' || record.status === 'pending') && (
            <Button type="text" danger icon={<StopOutlined />} onClick={() => handleCancelMigration(record)}>
              取消
            </Button>
          )}
        </Space>
      ),
    },
  ]

  const renderDeviceDetail = () => {
    if (!selectedDevice) return null
    const statusCfg = statusConfig[selectedDevice.status] || { color: 'default', text: selectedDevice.status }
    const healthCfg = selectedDevice.health
      ? healthConfig[selectedDevice.health] || { color: 'default', icon: null, text: selectedDevice.health }
      : null
    const usagePercent = selectedDevice.total_capacity > 0
      ? (selectedDevice.used_space / selectedDevice.total_capacity) * 100
      : 0

    return (
      <Space direction="vertical" style={{ width: '100%', gap: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ background: '#e6f7ff', padding: 12, borderRadius: 12 }}>
            <HddOutlined style={{ fontSize: 32, color: '#1890ff' }} />
          </div>
          <div>
            <h3 style={{ margin: 0 }}>{selectedDevice.device_id}</h3>
            <p style={{ margin: '4px 0', color: '#8c8c8c' }}>
              所属节点: {selectedDevice.location.node_id}
            </p>
          </div>
        </div>

        <div>
          <h4 style={{ margin: '0 0 12px' }}>存储使用</h4>
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <span style={{ color: '#8c8c8c' }}>已用空间</span>
              <span>{formatBytes(selectedDevice.used_space)}</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
              <span style={{ color: '#8c8c8c' }}>总容量</span>
              <span>{formatBytes(selectedDevice.total_capacity)}</span>
            </div>
            <Progress
              percent={parseFloat(usagePercent.toFixed(1))}
              strokeColor={usagePercent > 80 ? '#f5222d' : '#52c41a'}
            />
          </div>
        </div>

        <div>
          <h4 style={{ margin: '0 0 12px' }}>基本信息</h4>
          <Row gutter={[16, 12]}>
            <Col span={12}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>设备类型</span>
              <p style={{ margin: '4px 0', fontWeight: 500 }}>
                {deviceTypeMap[selectedDevice.device_type] || selectedDevice.device_type}
              </p>
            </Col>
            <Col span={12}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>状态</span>
              <p style={{ margin: '4px 0' }}>
                <Tag color={statusCfg.color}>{statusCfg.text}</Tag>
              </p>
            </Col>
            <Col span={12}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>健康度</span>
              <p style={{ margin: '4px 0' }}>
                {healthCfg ? (
                  <Tag color={healthCfg.color}>
                    {healthCfg.icon} {healthCfg.text}
                  </Tag>
                ) : '-'}
              </p>
            </Col>
            <Col span={12}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>Volume数量</span>
              <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedDevice.volume_count ?? 0} 个</p>
            </Col>
            <Col span={12}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>可用空间</span>
              <p style={{ margin: '4px 0', fontWeight: 500 }}>{formatBytes(selectedDevice.free_space)}</p>
            </Col>
            <Col span={12}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>最后检查</span>
              <p style={{ margin: '4px 0', fontWeight: 500 }}>
                {selectedDevice.last_check ? formatTime(selectedDevice.last_check) : '-'}
              </p>
            </Col>
          </Row>
        </div>

        <div>
          <h4 style={{ margin: '0 0 12px' }}>位置信息</h4>
          <Row gutter={[16, 12]}>
            <Col span={8}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>数据中心</span>
              <p style={{ margin: '4px 0', fontWeight: 500 }}>
                {selectedDevice.location.data_center || '-'}
              </p>
            </Col>
            <Col span={8}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>可用区</span>
              <p style={{ margin: '4px 0', fontWeight: 500 }}>
                {selectedDevice.location.zone || '-'}
              </p>
            </Col>
            <Col span={8}>
              <span style={{ color: '#8c8c8c', fontSize: 12 }}>机架</span>
              <p style={{ margin: '4px 0', fontWeight: 500 }}>
                {selectedDevice.location.rack || '-'}
              </p>
            </Col>
          </Row>
        </div>
      </Space>
    )
  }

  return (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            存储设备是 PowerFS 集群中的物理存储单元，包括本地磁盘、SPDK、NVMe-oF 等类型。系统会自动在多个设备之间分配数据，实现数据冗余和负载均衡。
          </Text>
        </div>
      </Card>

      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card style={{ borderRadius: 12 }}>
            <Statistic
              title="设备总数"
              value={deviceStats.total}
              prefix={<HddOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card style={{ borderRadius: 12 }}>
            <Statistic
              title="在线设备"
              value={deviceStats.online}
              valueStyle={{ color: '#52c41a' }}
              prefix={<CheckCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card style={{ borderRadius: 12 }}>
            <Statistic
              title="异常设备"
              value={deviceStats.offline}
              valueStyle={{ color: '#f5222d' }}
              prefix={<CloseCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card style={{ borderRadius: 12 }}>
            <Statistic
              title="总容量"
              value={deviceStats.totalCapacity}
              formatter={val => formatBytes(Number(val))}
            />
          </Card>
        </Col>
      </Row>

      <Card style={{ borderRadius: 12 }}>
        <Tabs
          items={[
            {
              key: 'devices',
              label: (
                <span>
                  <HddOutlined /> 存储设备 ({filteredDevices.length})
                </span>
              ),
              children: (
                <div>
                  <Space style={{ marginBottom: 16 }}>
                    <Tooltip title="刷新">
                      <Button icon={<ReloadOutlined />} onClick={() => { loadDevices(); loadMigrations(); }}>刷新</Button>
                    </Tooltip>
                    <Select
                      placeholder="按类型筛选"
                      style={{ width: 150 }}
                      value={filterType || undefined}
                      onChange={setFilterType}
                      options={[
                        { value: '', label: '全部类型' },
                        ...deviceTypes.map(t => ({ value: t, label: t })),
                      ]}
                    />
                    <Select
                      placeholder="按状态筛选"
                      style={{ width: 150 }}
                      value={filterStatus || undefined}
                      onChange={setFilterStatus}
                      options={[
                        { value: '', label: '全部状态' },
                        { value: 'online', label: '在线' },
                        { value: 'offline', label: '离线' },
                        { value: 'excluded', label: '已排除' },
                        { value: 'draining', label: '排空中' },
                        { value: 'faulty', label: '故障' },
                      ]}
                    />
                    <Select
                      placeholder="按节点筛选"
                      style={{ width: 150 }}
                      value={filterNode || undefined}
                      onChange={setFilterNode}
                      options={[
                        { value: '', label: '全部节点' },
                      ]}
                    />
                    <Button type="primary" icon={<PlusOutlined />}>
                      添加设备
                    </Button>
                  </Space>
                  <Table
                    columns={deviceColumns}
                    dataSource={filteredDevices}
                    rowKey="device_id"
                    pagination={{ pageSize: 10 }}
                    scroll={{ x: 1400 }}
                  />
                </div>
              ),
            },
            {
              key: 'migrations',
              label: (
                <span>
                  <ReloadOutlined /> 数据迁移 ({migrationTasks.length})
                </span>
              ),
              children: (
                <Table
                  columns={migrationColumns}
                  dataSource={migrationTasks}
                  rowKey="task_id"
                  pagination={{ pageSize: 10 }}
                  scroll={{ x: 1200 }}
                  locale={{ emptyText: '暂无迁移任务' }}
                />
              ),
            },
          ]}
        />
      </Card>

      <Modal
        title="设备详情"
        open={showDetail}
        onCancel={() => setShowDetail(false)}
        footer={null}
        width={700}
        destroyOnClose
      >
        {renderDeviceDetail()}
      </Modal>
    </div>
  )
}

export default StorageDevices
