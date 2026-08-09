import { useEffect, useState } from 'react'
import { Card, Table, Tag, Button, Modal, Space, Progress, Select, message, Tooltip, Typography, Descriptions, Tabs, Statistic, Row, Col, Empty } from 'antd'
import {
  DatabaseOutlined,
  DeleteOutlined,
  EyeOutlined,
  FireOutlined,
  ReloadOutlined,
  InfoCircleOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { VolumeInfo, VolumeIoStats } from '@/types'
import { getVolumes, getVolumeIo } from '@/services/api'
import { formatBytes } from '@/utils/format'
import { useMetricStream } from '@/hooks/useMetricStream'

const { Text } = Typography

function Volumes() {
  const { t } = useTranslation(['common', 'nav'])
  const [volumes, setVolumes] = useState<VolumeInfo[]>([])
  const [selectedVolume, setSelectedVolume] = useState<VolumeInfo | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [showMigrate, setShowMigrate] = useState(false)
  const [filterStatus, setFilterStatus] = useState<string>('')
  const [filterCollection, setFilterCollection] = useState<string>('')
  const [activeTab, setActiveTab] = useState<'volumes' | 'io'>('volumes')
  const [ioStats, setIoStats] = useState<VolumeIoStats[]>([])
  const [ioLoading, setIoLoading] = useState(false)

  const loadVolumes = async () => {
    const data = await getVolumes()
    console.log('Loaded volumes:', data.length, 'unique:', new Set(data.map(v => v.id)).size)
    setVolumes(data)
  }

  const loadAllIo = async () => {
    if (volumes.length === 0) return
    setIoLoading(true)
    try {
      const results = await Promise.allSettled(volumes.map(v => getVolumeIo(v.id)))
      const ok: VolumeIoStats[] = []
      for (const r of results) {
        if (r.status === 'fulfilled' && r.value) ok.push(r.value)
      }
      setIoStats(ok)
    } catch (e) {
      console.error('Failed to load volume IO:', e)
    } finally {
      setIoLoading(false)
    }
  }

  useEffect(() => {
    loadVolumes()
    // Backoff polling to 30s — real-time updates flow through WS.
    const interval = setInterval(loadVolumes, 30000)
    return () => clearInterval(interval)
  }, [])

  // Load IO stats when user switches to the IO tab (lazy).
  useEffect(() => {
    if (activeTab === 'io' && ioStats.length === 0 && volumes.length > 0) {
      void loadAllIo()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, volumes.length])

  // Live merge: WS pushes either a single VolumeInfo (event-driven) or the
  // full list snapshot (on connect). Both update by volume id.
  useMetricStream({
    source: 'volumes',
    onMetricUpdate: (u) => {
      const payload = u.payload
      if (Array.isArray(payload)) {
        setVolumes(payload as VolumeInfo[])
      } else if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
        const vol = payload as VolumeInfo
        setVolumes((prev) => {
          const idx = prev.findIndex((v) => v.id === vol.id)
          if (idx === -1) return [...prev, vol]
          const next = [...prev]
          next[idx] = { ...next[idx], ...vol }
          return next
        })
      }
    },
  })

  const handleViewDetail = (volume: VolumeInfo) => {
    setSelectedVolume(volume)
    setShowDetail(true)
  }

  // TODO: restore delete handler after DELETE /metrics/volumes/:id endpoint is added (decision 3)
  // const handleDelete = (volume: VolumeInfo) => {
  //   setSelectedVolume(volume)
  //   setShowDeleteConfirm(true)
  // }

  const handleMigrate = (volume: VolumeInfo) => {
    setSelectedVolume(volume)
    setShowMigrate(true)
  }

  const confirmDelete = async () => {
    // TODO: restore after DELETE /metrics/volumes/:id endpoint is added (decision 3)
    if (selectedVolume) {
      message.warning('Volume 删除暂不可用（后端 DELETE 接口待补充）')
      setShowDeleteConfirm(false)
    }
  }

  const collections = [...new Set(volumes.map(v => v.collection))]
  const filteredVolumes = volumes
    .filter(v => {
      if (filterStatus && v.status !== filterStatus) return false
      if (filterCollection && v.collection !== filterCollection) return false
      return true
    })
    .reduce((acc, v) => {
      if (!acc.find(item => item.id === v.id)) {
        acc.push(v)
      }
      return acc
    }, [] as VolumeInfo[])

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
          <Tooltip title="后端 DELETE 路由待补充，暂不可用">
            <Button
              type="text"
              danger
              icon={<DeleteOutlined />}
              disabled
            >
              删除
            </Button>
          </Tooltip>
        </Space>
      ),
    },
  ]

  return (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            Volume 是 PowerFS 的物理存储单元，每个 Volume 位于一个节点上。系统会自动在多个 Volume 之间分配数据，实现数据冗余和负载均衡。
          </Text>
        </div>
      </Card>

      <Tabs
        activeKey={activeTab}
        onChange={(k) => setActiveTab(k as 'volumes' | 'io')}
        items={[
          {
            key: 'volumes',
            label: <span><DatabaseOutlined /> {t('common:volumeManagement')}</span>,
            children: renderVolumesTab(),
          },
          {
            key: 'io',
            label: <span><ThunderboltOutlined /> {t('common:ioPerformance')}</span>,
            children: renderIoTab(),
          },
        ]}
      />

      {renderModals()}

      <Card title={t('common:info')} size="small" style={{ marginTop: 24 }}>
        <Descriptions column={1} size="small">
          <Descriptions.Item label="什么是 Volume？">
            Volume 是 PowerFS 的物理存储单元，每个 Volume 位于一个节点上。Volume 类似于传统文件系统中的磁盘分区或逻辑卷。
          </Descriptions.Item>
          <Descriptions.Item label="什么是 Collection？">
            Collection 是逻辑数据集合，用于隔离不同应用或用户的数据。同一个 Collection 的数据会分布在多个 Volume 上。
          </Descriptions.Item>
          <Descriptions.Item label="为什么 Volume 会显示只读？">
            当 Volume 所在节点出现故障或网络中断时，为了保证数据一致性，系统会将该 Volume 标记为只读状态。
          </Descriptions.Item>
          <Descriptions.Item label="如何迁移 Volume？">
            通过"迁移"操作可以将 Volume 从一个节点迁移到另一个节点。迁移过程中数据会被复制到目标节点，原 Volume 会被删除。
          </Descriptions.Item>
        </Descriptions>
      </Card>
    </div>
  )

  function renderVolumesTab() {
    return (
      <Card
        title={t('common:volumeManagement')}
        style={{ borderRadius: 12, marginBottom: 16 }}
        styles={{ body: { paddingBottom: 16 } }}
        extra={
          <Tooltip title={t('common:refresh')}>
            <Button icon={<ReloadOutlined />} onClick={loadVolumes}>{t('common:refresh')}</Button>
          </Tooltip>
        }
      >
        <Space style={{ marginBottom: 16 }}>
          <Select
            placeholder={t('common:status')}
            style={{ width: 150 }}
            value={filterStatus || undefined}
            onChange={setFilterStatus}
            options={[
              { value: '', label: t('common:all') },
              { value: 'available', label: 'Available' },
              { value: 'full', label: 'Full' },
              { value: 'read_only', label: 'Read-only' },
              { value: 'creating', label: 'Creating' },
            ]}
          />
          <Select
            placeholder="Collection"
            style={{ width: 150 }}
            value={filterCollection || undefined}
            onChange={setFilterCollection}
            options={[
              { value: '', label: t('common:all') },
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
    )
  }

  function renderIoTab() {
    // Aggregate KPIs across all volumes.
    const totals = ioStats.reduce(
      (acc, s) => ({
        read_ops: acc.read_ops + s.read_ops,
        write_ops: acc.write_ops + s.write_ops,
        read_bytes: acc.read_bytes + s.read_bytes,
        write_bytes: acc.write_bytes + s.write_bytes,
        read_lat_sum: acc.read_lat_sum + s.read_avg_latency_us,
        write_lat_sum: acc.write_lat_sum + s.write_avg_latency_us,
        count: acc.count + 1,
      }),
      { read_ops: 0, write_ops: 0, read_bytes: 0, write_bytes: 0, read_lat_sum: 0, write_lat_sum: 0, count: 0 },
    )
    const avgReadLat = totals.count > 0 ? totals.read_lat_sum / totals.count : 0
    const avgWriteLat = totals.count > 0 ? totals.write_lat_sum / totals.count : 0

    const ioColumns = [
      { title: 'Volume ID', dataIndex: 'volume_id', key: 'volume_id', width: 200,
        render: (id: number) => <strong>{id}</strong> },
      { title: t('common:readOps'), dataIndex: 'read_ops', key: 'read_ops', width: 120,
        render: (v: number) => v.toLocaleString() },
      { title: t('common:writeOps'), dataIndex: 'write_ops', key: 'write_ops', width: 120,
        render: (v: number) => v.toLocaleString() },
      { title: t('common:readBytes'), dataIndex: 'read_bytes', key: 'read_bytes', width: 120,
        render: (v: number) => formatBytes(v) },
      { title: t('common:writeBytes'), dataIndex: 'write_bytes', key: 'write_bytes', width: 120,
        render: (v: number) => formatBytes(v) },
      { title: t('common:readAvgLatency'), dataIndex: 'read_avg_latency_us', key: 'read_avg_latency_us', width: 140,
        render: (v: number) => v > 0 ? `${(v / 1000).toFixed(2)} ms` : '-' },
      { title: t('common:writeAvgLatency'), dataIndex: 'write_avg_latency_us', key: 'write_avg_latency_us', width: 140,
        render: (v: number) => v > 0 ? `${(v / 1000).toFixed(2)} ms` : '-' },
    ]

    return (
      <div>
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={t('common:totalReadOps')}
                value={totals.read_ops}
                valueStyle={{ color: '#1890ff' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={t('common:totalWriteOps')}
                value={totals.write_ops}
                valueStyle={{ color: '#52c41a' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={t('common:totalReadBytes')}
                value={totals.read_bytes}
                formatter={(v) => formatBytes(Number(v))}
                valueStyle={{ color: '#1890ff' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={t('common:totalWriteBytes')}
                value={totals.write_bytes}
                formatter={(v) => formatBytes(Number(v))}
                valueStyle={{ color: '#52c41a' }}
              />
            </Card>
          </Col>
        </Row>

        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={t('common:avgReadLatency')}
                value={avgReadLat / 1000}
                precision={2}
                suffix="ms"
                valueStyle={{ color: '#faad14' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title={t('common:avgWriteLatency')}
                value={avgWriteLat / 1000}
                precision={2}
                suffix="ms"
                valueStyle={{ color: '#faad14' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic title={t('common:volume')} value={ioStats.length} />
            </Card>
          </Col>
          <Col span={6}>
            <Tooltip title={t('common:refresh')}>
              <Button
                icon={<ReloadOutlined />}
                onClick={loadAllIo}
                loading={ioLoading}
                style={{ marginTop: 16 }}
              >
                {t('common:refresh')}
              </Button>
            </Tooltip>
          </Col>
        </Row>

        <Card
          title={`${t('common:ioPerformance')} — ${t('common:volume')}`}
          style={{ borderRadius: 12 }}
        >
          {ioStats.length === 0 && !ioLoading ? (
            <Empty description={t('common:noData')} />
          ) : (
            <Table
              columns={ioColumns}
              dataSource={ioStats}
              rowKey="volume_id"
              pagination={{ pageSize: 10 }}
              loading={ioLoading}
              scroll={{ x: 1000 }}
            />
          )}
        </Card>
      </div>
    )
  }

  function renderModals() {
    return (
      <>
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
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>状态</span>
                  <div>
                    <Tag color={selectedVolume.status === 'available' ? 'green' : selectedVolume.status === 'full' ? 'red' : selectedVolume.status === 'read_only' ? 'orange' : 'blue'}>
                      {selectedVolume.status === 'available' ? '可用' : selectedVolume.status === 'full' ? '已满' : selectedVolume.status === 'read_only' ? '只读' : selectedVolume.status === 'deleting' ? '删除中' : '创建中'}
                    </Tag>
                  </div>
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

            <div>
              <h4 style={{ margin: '0 0 12px' }}>存储配置</h4>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>只读模式</span>
                  <div>
                    {selectedVolume.read_only ? <Tag color="orange">只读</Tag> : <Tag color="green">读写</Tag>}
                  </div>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>副本数</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedVolume.replica_placement ?? '-'}</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>TTL</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedVolume.ttl ? `${selectedVolume.ttl}s` : '-'}</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>磁盘类型</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedVolume.disk_type || '-'}</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>追加偏移</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedVolume.append_offset ? formatBytes(selectedVolume.append_offset) : '-'}</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>压缩状态</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedVolume.compact_status ?? 0}</p>
                </div>
              </div>
            </div>

            {selectedVolume.read_ops !== undefined && selectedVolume.read_ops > 0 && (
              <div>
                <h4 style={{ margin: '0 0 12px' }}>I/O 性能 (累计)</h4>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                  <div>
                    <span style={{ color: '#8c8c8c', fontSize: 12 }}>读操作</span>
                    <p style={{ margin: '4px 0', fontWeight: 500 }}>{(selectedVolume.read_ops ?? 0).toLocaleString()}</p>
                  </div>
                  <div>
                    <span style={{ color: '#8c8c8c', fontSize: 12 }}>写操作</span>
                    <p style={{ margin: '4px 0', fontWeight: 500 }}>{(selectedVolume.write_ops ?? 0).toLocaleString()}</p>
                  </div>
                  <div>
                    <span style={{ color: '#8c8c8c', fontSize: 12 }}>读流量</span>
                    <p style={{ margin: '4px 0', fontWeight: 500 }}>{formatBytes(selectedVolume.read_bytes ?? 0)}</p>
                  </div>
                  <div>
                    <span style={{ color: '#8c8c8c', fontSize: 12 }}>写流量</span>
                    <p style={{ margin: '4px 0', fontWeight: 500 }}>{formatBytes(selectedVolume.write_bytes ?? 0)}</p>
                  </div>
                </div>

                <h4 style={{ margin: '16px 0 12px' }}>延迟分布 (最近采样)</h4>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                  <div style={{ padding: '8px', background: '#e6f4ff', borderRadius: 4 }}>
                    <div style={{ color: '#8c8c8c', fontSize: 12, marginBottom: 4 }}>读取</div>
                    <div>平均: {selectedVolume.read_avg_latency_us ? `${(selectedVolume.read_avg_latency_us / 1000).toFixed(1)}ms` : '-'}</div>
                    <div>P50: {selectedVolume.read_p50_latency_us ? `${(selectedVolume.read_p50_latency_us / 1000).toFixed(1)}ms` : '-'}</div>
                    <div>P99: {selectedVolume.read_p99_latency_us ? `${(selectedVolume.read_p99_latency_us / 1000).toFixed(1)}ms` : '-'}</div>
                  </div>
                  <div style={{ padding: '8px', background: '#f6ffed', borderRadius: 4 }}>
                    <div style={{ color: '#8c8c8c', fontSize: 12, marginBottom: 4 }}>写入</div>
                    <div>平均: {selectedVolume.write_avg_latency_us ? `${(selectedVolume.write_avg_latency_us / 1000).toFixed(1)}ms` : '-'}</div>
                    <div>P50: {selectedVolume.write_p50_latency_us ? `${(selectedVolume.write_p50_latency_us / 1000).toFixed(1)}ms` : '-'}</div>
                    <div>P99: {selectedVolume.write_p99_latency_us ? `${(selectedVolume.write_p99_latency_us / 1000).toFixed(1)}ms` : '-'}</div>
                  </div>
                </div>
              </div>
            )}
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
      </>
    )
  }
}

export default Volumes