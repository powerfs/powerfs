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
  Select,
  Radio,
} from 'antd'
import {
  PlusOutlined,
  ReloadOutlined,
  DeleteOutlined,
  InfoCircleOutlined,
  DatabaseOutlined,
  EditOutlined,
} from '@ant-design/icons'
import type {
  CollectionInfo,
  CollectionStats,
  StoragePolicyInfo,
  VolumeAllocationInfo,
} from '@/types'
import {
  getCollections,
  createCollection,
  updateCollection,
  deleteCollection,
  getCollectionStats,
  type CreateCollectionParams,
  type UpdateCollectionParams,
  type StoragePolicyParams,
  type VolumeAllocationParams,
} from '@/services/api'
import { formatNumber } from '@/utils/format'

const { Text } = Typography

// 状态选项（不含 Deleted，删除态不在表单中选择）
const STATUS_OPTIONS = [
  { label: 'Active (正常)', value: 1 },
  { label: 'Readonly (只读)', value: 2 },
  { label: 'Archived (归档)', value: 3 },
]

// 磁盘类型选项
const DISK_TYPE_OPTIONS = [
  { label: 'HDD', value: 'HDD' },
  { label: 'SSD', value: 'SSD' },
  { label: 'NVMe', value: 'NVMe' },
  { label: 'Mixed', value: 'Mixed' },
]

// EC 算法选项
const EC_ALGORITHM_OPTIONS = [
  { label: 'Reed-Solomon', value: 'reed_solomon' },
]

const GB = 1024 * 1024 * 1024

// 字节格式化（含 PB）
function formatBytes(bytes: number): string {
  if (!bytes || bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

// 冗余策略展示
function formatRedundancy(policy: StoragePolicyInfo | null): string {
  if (!policy) return '-'
  const r = policy.redundancy
  if (r.mode === 'erasure_coding') {
    return `EC ${r.data_shards ?? '?'}+${r.parity_shards ?? '?'}`
  }
  return `副本 x${r.copies ?? 1}`
}

// 状态标签映射
function statusTag(status_name: string): { color: string; text: string } {
  switch ((status_name || '').toLowerCase()) {
    case 'active': return { color: 'green', text: '正常' }
    case 'readonly': return { color: 'orange', text: '只读' }
    case 'archived': return { color: 'gray', text: '归档' }
    case 'deleted': return { color: 'red', text: '已删除' }
    default: return { color: 'default', text: status_name || '-' }
  }
}

// 时间戳格式化（后端返回秒）
function formatTimestamp(ts: number): string {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString()
}

// 解析逗号分隔的 ID 列表
function parseIdList(str: string | undefined | null): number[] {
  if (!str) return []
  return str
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => Number(s))
    .filter((n) => !isNaN(n))
}

// Volume 分配策略展示
function formatAllocation(alloc: VolumeAllocationInfo | null): string {
  if (!alloc) return '-'
  if (alloc.mode === 'auto') {
    return `自动 (数量: ${alloc.count ?? 0}, 单卷: ${formatBytes(alloc.volume_size ?? 0)})`
  }
  if (alloc.mode === 'manual') {
    return `手动 (IDs: ${alloc.volume_ids && alloc.volume_ids.length ? alloc.volume_ids.join(', ') : '-'})`
  }
  if (alloc.mode === 'hybrid') {
    return `混合 (固定IDs: ${alloc.fixed_volume_ids && alloc.fixed_volume_ids.length ? alloc.fixed_volume_ids.join(', ') : '-'}, 自动补充: ${alloc.auto_count ?? 0})`
  }
  return alloc.mode
}

function Collections() {
  const [collections, setCollections] = useState<CollectionInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [formVisible, setFormVisible] = useState(false)
  const [formMode, setFormMode] = useState<'create' | 'edit'>('create')
  const [editingName, setEditingName] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [detailRecord, setDetailRecord] = useState<CollectionInfo | null>(null)
  const [detailStats, setDetailStats] = useState<CollectionStats | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [form] = Form.useForm()

  // 监听冗余模式与分配模式，用于条件渲染
  const redundancyMode = Form.useWatch('redundancyMode', form)
  const allocationMode = Form.useWatch('allocationMode', form)

  useEffect(() => {
    loadData()
  }, [])

  // 详情打开时拉取容量统计
  useEffect(() => {
    if (detailRecord) {
      setDetailStats(null)
      setDetailLoading(true)
      getCollectionStats(detailRecord.name)
        .then((stats) => setDetailStats(stats))
        .catch((err) => {
          console.error('Failed to load collection stats:', err)
        })
        .finally(() => setDetailLoading(false))
    }
  }, [detailRecord])

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

  const openCreate = () => {
    form.resetFields()
    setFormMode('create')
    setEditingName(null)
    setFormVisible(true)
  }

  const openEdit = (record: CollectionInfo) => {
    const r = record.storage_policy?.redundancy
    const alloc = record.volume_allocation
    let allocMode: 'auto' | 'manual' | 'hybrid' = 'auto'
    if (alloc?.mode === 'manual') allocMode = 'manual'
    else if (alloc?.mode === 'hybrid') allocMode = 'hybrid'

    form.setFieldsValue({
      name: record.name,
      description: record.description,
      status: record.status,
      redundancyMode: r?.mode === 'erasure_coding' ? 'erasure_coding' : 'replication',
      copies: r?.copies ?? 1,
      data_shards: r?.data_shards ?? 4,
      parity_shards: r?.parity_shards ?? 2,
      algorithm: r?.algorithm ?? 'reed_solomon',
      disk_type: record.disk_type || 'HDD',
      capacityQuotaGb: record.capacity_quota_bytes ? record.capacity_quota_bytes / GB : 0,
      ttl_seconds: record.ttl_seconds ?? 0,
      allocationMode: allocMode,
      allocationCount: alloc?.count ?? 0,
      volumeSizeGb: alloc?.volume_size ? alloc.volume_size / GB : 0,
      volumeIds: alloc?.volume_ids?.join(', ') ?? '',
      fixedVolumeIds: alloc?.fixed_volume_ids?.join(', ') ?? '',
      autoCount: alloc?.auto_count ?? 0,
      excludedVolumeIds: record.excluded_volume_ids?.join(', ') ?? '',
    })
    setFormMode('edit')
    setEditingName(record.name)
    setFormVisible(true)
  }

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields()
      setSubmitting(true)

      const redMode = (values.redundancyMode as 'replication' | 'erasure_coding') || 'replication'
      const storagePolicy: StoragePolicyParams = {
        name: 'default',
        redundancy:
          redMode === 'replication'
            ? { mode: 'replication', copies: values.copies ?? 1 }
            : {
                mode: 'erasure_coding',
                data_shards: values.data_shards ?? 4,
                parity_shards: values.parity_shards ?? 2,
                algorithm: values.algorithm ?? 'reed_solomon',
              },
        min_write_nodes: 1,
      }

      const allocMode = (values.allocationMode as 'auto' | 'manual' | 'hybrid') || 'auto'
      let volumeAllocation: VolumeAllocationParams
      if (allocMode === 'manual') {
        volumeAllocation = {
          mode: 'manual',
          volume_ids: parseIdList(values.volumeIds),
        }
      } else if (allocMode === 'hybrid') {
        volumeAllocation = {
          mode: 'hybrid',
          fixed_volume_ids: parseIdList(values.fixedVolumeIds),
          auto_count: values.autoCount ?? 0,
        }
      } else {
        volumeAllocation = {
          mode: 'auto',
          count: values.allocationCount ?? 0,
          volume_size: (values.volumeSizeGb ?? 0) * GB,
        }
      }

      const capacityQuotaBytes = (values.capacityQuotaGb ?? 0) * GB
      const excludedVolumeIds = parseIdList(values.excludedVolumeIds)

      if (formMode === 'create') {
        const params: CreateCollectionParams = {
          name: values.name,
          status: values.status ?? 1,
          storage_policy: storagePolicy,
          disk_type: values.disk_type ?? 'HDD',
          capacity_quota_bytes: capacityQuotaBytes,
          volume_count: values.volume_count ?? 0,
          ttl_seconds: values.ttl_seconds ?? 0,
          description: values.description ?? '',
          volume_allocation: volumeAllocation,
          excluded_volume_ids: excludedVolumeIds,
        }
        await createCollection(params)
        message.success(`Collection ${params.name} 创建成功`)
      } else {
        const params: UpdateCollectionParams = {
          status: values.status ?? 1,
          storage_policy: storagePolicy,
          disk_type: values.disk_type ?? 'HDD',
          capacity_quota_bytes: capacityQuotaBytes,
          ttl_seconds: values.ttl_seconds ?? 0,
          description: values.description ?? '',
          volume_allocation: volumeAllocation,
          excluded_volume_ids: excludedVolumeIds,
        }
        await updateCollection(editingName!, params)
        message.success(`Collection ${editingName} 更新成功`)
      }
      setFormVisible(false)
      form.resetFields()
      loadData()
    } catch (error: any) {
      if (error?.errorFields) return // 表单校验错误
      const msg = error?.response?.data?.message || error?.message || '操作失败'
      message.error(msg)
    } finally {
      setSubmitting(false)
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
      title: '状态',
      dataIndex: 'status_name',
      key: 'status_name',
      render: (status_name: string) => {
        const tag = statusTag(status_name)
        return <Tag color={tag.color}>{tag.text}</Tag>
      },
    },
    {
      title: '存储策略',
      key: 'storage_policy',
      render: (_: any, record: CollectionInfo) => (
        <Tag color="blue">{formatRedundancy(record.storage_policy)}</Tag>
      ),
    },
    {
      title: '磁盘类型',
      dataIndex: 'disk_type',
      key: 'disk_type',
      render: (d: string) => (d ? <Tag>{d}</Tag> : <Text type="secondary">-</Text>),
    },
    {
      title: '容量配额',
      dataIndex: 'capacity_quota_bytes',
      key: 'capacity_quota_bytes',
      render: (v: number) =>
        v > 0 ? formatBytes(v) : <Text type="secondary">无限制</Text>,
    },
    {
      title: 'Volume 数',
      dataIndex: 'volume_count',
      key: 'volume_count',
      render: (v: number) => formatNumber(v),
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
          <Tooltip title="编辑">
            <Button
              type="text"
              icon={<EditOutlined />}
              onClick={() => openEdit(record)}
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
            <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
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

      {/* 新建 / 编辑 Modal */}
      <Modal
        title={formMode === 'create' ? '新建 Collection' : `编辑 Collection: ${editingName}`}
        open={formVisible}
        onOk={handleSubmit}
        confirmLoading={submitting}
        onCancel={() => {
          setFormVisible(false)
          form.resetFields()
        }}
        okText={formMode === 'create' ? '创建' : '保存'}
        cancelText="取消"
        width={720}
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{
            status: 1,
            redundancyMode: 'replication',
            copies: 1,
            data_shards: 4,
            parity_shards: 2,
            algorithm: 'reed_solomon',
            disk_type: 'HDD',
            capacityQuotaGb: 0,
            volume_count: 0,
            ttl_seconds: 0,
            allocationMode: 'auto',
            allocationCount: 0,
            volumeSizeGb: 0,
            autoCount: 0,
          }}
        >
          <Form.Item
            name="name"
            label="Collection 名称"
            rules={[{ required: true, message: '请输入名称' }]}
          >
            <Input placeholder="例如 ml-cache" disabled={formMode === 'edit'} />
          </Form.Item>

          <Form.Item name="description" label="描述">
            <Input placeholder="可选描述" />
          </Form.Item>

          <Form.Item name="status" label="状态">
            <Select options={STATUS_OPTIONS} />
          </Form.Item>

          <Form.Item name="redundancyMode" label="冗余模式">
            <Radio.Group>
              <Radio value="replication">副本</Radio>
              <Radio value="erasure_coding">EC 纠删码</Radio>
            </Radio.Group>
          </Form.Item>

          {redundancyMode === 'erasure_coding' ? (
            <Space style={{ display: 'flex' }} align="start" wrap>
              <Form.Item name="data_shards" label="数据分片数" rules={[{ required: true, message: '请输入' }]}>
                <InputNumber min={1} placeholder="4" />
              </Form.Item>
              <Form.Item name="parity_shards" label="校验分片数" rules={[{ required: true, message: '请输入' }]}>
                <InputNumber min={1} placeholder="2" />
              </Form.Item>
              <Form.Item name="algorithm" label="算法">
                <Select options={EC_ALGORITHM_OPTIONS} style={{ width: 160 }} />
              </Form.Item>
            </Space>
          ) : (
            <Form.Item name="copies" label="副本数" rules={[{ required: true, message: '请输入' }]}>
              <InputNumber min={1} placeholder="1" />
            </Form.Item>
          )}

          <Form.Item name="disk_type" label="磁盘类型">
            <Select options={DISK_TYPE_OPTIONS} />
          </Form.Item>

          <Form.Item name="capacityQuotaGb" label="容量配额 (GB)" tooltip="0 表示无限制">
            <InputNumber min={0} style={{ width: '100%' }} placeholder="0" />
          </Form.Item>

          {formMode === 'create' && (
            <Form.Item name="volume_count" label="预分配 Volume 数量" tooltip="0 表示不预分配（不可更新）">
              <InputNumber min={0} style={{ width: '100%' }} placeholder="0" />
            </Form.Item>
          )}

          <Form.Item name="ttl_seconds" label="TTL 秒数" tooltip="0 表示永不过期">
            <InputNumber min={0} style={{ width: '100%' }} placeholder="0" />
          </Form.Item>

          <Form.Item name="allocationMode" label="Volume 分配模式">
            <Radio.Group>
              <Radio value="auto">自动</Radio>
              <Radio value="manual">手动</Radio>
              <Radio value="hybrid">混合</Radio>
            </Radio.Group>
          </Form.Item>

          {allocationMode === 'auto' && (
            <Space style={{ display: 'flex' }} align="start" wrap>
              <Form.Item name="allocationCount" label="预分配数量">
                <InputNumber min={0} placeholder="0" />
              </Form.Item>
              <Form.Item name="volumeSizeGb" label="单 Volume 大小 (GB)">
                <InputNumber min={0} placeholder="0" />
              </Form.Item>
            </Space>
          )}

          {allocationMode === 'manual' && (
            <Form.Item name="volumeIds" label="Volume ID 列表" tooltip="逗号分隔，如 1, 2, 3">
              <Input placeholder="例如 1, 2, 3" />
            </Form.Item>
          )}

          {allocationMode === 'hybrid' && (
            <Space style={{ display: 'flex' }} align="start" wrap>
              <Form.Item name="fixedVolumeIds" label="固定 Volume ID" tooltip="逗号分隔">
                <Input placeholder="例如 1, 2" />
              </Form.Item>
              <Form.Item name="autoCount" label="自动补充数量">
                <InputNumber min={0} placeholder="0" />
              </Form.Item>
            </Space>
          )}

          <Form.Item name="excludedVolumeIds" label="排除 Volume ID" tooltip="可选，逗号分隔">
            <Input placeholder="例如 4, 5" />
          </Form.Item>
        </Form>
      </Modal>

      {/* 详情 Modal */}
      <Modal
        title="Collection 详情"
        open={!!detailRecord}
        onCancel={() => setDetailRecord(null)}
        footer={<Button onClick={() => setDetailRecord(null)}>关闭</Button>}
        width={720}
      >
        {detailRecord && (
          <Descriptions column={1} bordered size="small">
            <Descriptions.Item label="名称">{detailRecord.name}</Descriptions.Item>
            <Descriptions.Item label="状态">
              {(() => {
                const tag = statusTag(detailRecord.status_name)
                return <Tag color={tag.color}>{tag.text}</Tag>
              })()}
            </Descriptions.Item>
            <Descriptions.Item label="描述">{detailRecord.description || '-'}</Descriptions.Item>
            <Descriptions.Item label="存储策略">
              {formatRedundancy(detailRecord.storage_policy)}
              {detailRecord.storage_policy && (
                <Text type="secondary"> (min_write_nodes={detailRecord.storage_policy.min_write_nodes})</Text>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="磁盘类型">{detailRecord.disk_type || '-'}</Descriptions.Item>
            <Descriptions.Item label="容量配额">
              {detailRecord.capacity_quota_bytes > 0
                ? formatBytes(detailRecord.capacity_quota_bytes)
                : '无限制'}
            </Descriptions.Item>
            <Descriptions.Item label="Volume 数">{formatNumber(detailRecord.volume_count)}</Descriptions.Item>
            <Descriptions.Item label="TTL">
              {detailRecord.ttl_seconds > 0 ? `${detailRecord.ttl_seconds} 秒` : '永不过期'}
            </Descriptions.Item>
            <Descriptions.Item label="Volume 分配策略">
              {formatAllocation(detailRecord.volume_allocation)}
            </Descriptions.Item>
            <Descriptions.Item label="排除 Volume IDs">
              {detailRecord.excluded_volume_ids && detailRecord.excluded_volume_ids.length
                ? detailRecord.excluded_volume_ids.join(', ')
                : '-'}
            </Descriptions.Item>
            <Descriptions.Item label="创建时间">{formatTimestamp(detailRecord.created_at)}</Descriptions.Item>
            <Descriptions.Item label="更新时间">{formatTimestamp(detailRecord.updated_at)}</Descriptions.Item>

            {/* 容量与 I/O 统计 */}
            <Descriptions.Item label="已用容量">
              {detailLoading || !detailStats ? <Text type="secondary">加载中...</Text> : formatBytes(detailStats.used_bytes)}
            </Descriptions.Item>
            <Descriptions.Item label="文件数">
              {detailLoading || !detailStats ? <Text type="secondary">加载中...</Text> : formatNumber(detailStats.file_count)}
            </Descriptions.Item>
            <Descriptions.Item label="可写 Volume 数">
              {detailLoading || !detailStats ? <Text type="secondary">加载中...</Text> : formatNumber(detailStats.writable_volume_count)}
            </Descriptions.Item>
            <Descriptions.Item label="读 / 写 OPS">
              {detailLoading || !detailStats ? (
                <Text type="secondary">加载中...</Text>
              ) : (
                `${formatNumber(detailStats.read_ops)} / ${formatNumber(detailStats.write_ops)}`
              )}
            </Descriptions.Item>
            <Descriptions.Item label="读 / 写字节">
              {detailLoading || !detailStats ? (
                <Text type="secondary">加载中...</Text>
              ) : (
                `${formatBytes(detailStats.read_bytes)} / ${formatBytes(detailStats.write_bytes)}`
              )}
            </Descriptions.Item>
          </Descriptions>
        )}
      </Modal>
    </div>
  )
}

export default Collections
