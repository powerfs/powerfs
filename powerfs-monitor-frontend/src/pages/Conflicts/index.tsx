import { useState, useEffect, useCallback } from 'react'
import {
  Card,
  Table,
  Tag,
  Button,
  Space,
  Input,
  Select,
  Row,
  Col,
  Statistic,
  Modal,
  Descriptions,
  Popconfirm,
  message,
  Tooltip,
  Switch,
} from 'antd'
import {
  WarningOutlined,
  ReloadOutlined,
  ThunderboltOutlined,
  CheckCircleOutlined,
  StopOutlined,
  EyeOutlined,
  FilterOutlined,
} from '@ant-design/icons'
import type { ConflictRecord, ConflictStats } from '@/types'
import {
  getConflicts,
  getConflictStats,
  resolveConflict,
  autoResolveConflicts,
  batchResolveConflicts,
  batchIgnoreConflicts,
} from '@/services/api'

const CONFLICT_TYPE_LABELS: Record<number, { label: string; color: string }> = {
  0: { label: 'CreateCreate', color: 'blue' },
  1: { label: 'WriteWrite', color: 'orange' },
  2: { label: 'WriteUnlink', color: 'gold' },
  3: { label: 'DeleteCreate', color: 'volcano' },
  4: { label: 'RenameConflict', color: 'magenta' },
}

const RESOLUTION_LABELS: Record<number, string> = {
  0: '保留首个 (KeepFirst)',
  1: '保留最后 (KeepLast)',
  2: '全部保留 (KeepAll)',
  3: '合并 (Merge)',
}

const POLICY_LABELS: Record<number, string> = {
  0: 'LwwTime (最后写入胜出)',
  1: 'ContentHash (内容哈希)',
  2: 'WeightBased (权重)',
  3: 'KeepAll (全部保留)',
  4: 'WritePriority (写入优先)',
  5: 'DeletePriority (删除优先)',
  6: 'Aggressive (激进)',
  7: 'Conservative (保守)',
  8: 'Manual (手动)',
}

function formatTime(ts: number): string {
  if (!ts || ts === 0) return '-'
  return new Date(ts * 1000).toLocaleString()
}

function formatSize(bytes: number): string {
  if (!bytes) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function Conflicts() {
  const [conflicts, setConflicts] = useState<ConflictRecord[]>([])
  const [stats, setStats] = useState<ConflictStats | null>(null)
  const [loading, setLoading] = useState(false)
  const [dirPath, setDirPath] = useState('')
  const [unresolvedOnly, setUnresolvedOnly] = useState(true)
  const [detailVisible, setDetailVisible] = useState(false)
  const [currentConflict, setCurrentConflict] = useState<ConflictRecord | null>(null)
  const [resolveModalVisible, setResolveModalVisible] = useState(false)
  const [resolveConflictId, setResolveConflictId] = useState<string>('')
  const [resolveResolution, setResolveResolution] = useState<number>(0)
  const [autoResolveModalVisible, setAutoResolveModalVisible] = useState(false)
  const [autoResolvePolicy, setAutoResolvePolicy] = useState<number>(0)
  const [batchModalVisible, setBatchModalVisible] = useState(false)
  const [batchPolicy, setBatchPolicy] = useState<number>(0)
  const [batchConflictType, setBatchConflictType] = useState<number>(-1)

  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const [list, s] = await Promise.all([
        getConflicts({
          dir_path: dirPath || undefined,
          unresolved_only: unresolvedOnly,
        }),
        getConflictStats({
          dir_path: dirPath || undefined,
          recursive: true,
        }),
      ])
      setConflicts(list)
      setStats(s)
    } catch (error) {
      console.error('Failed to load conflicts:', error)
      message.error('加载冲突列表失败')
    } finally {
      setLoading(false)
    }
  }, [dirPath, unresolvedOnly])

  useEffect(() => {
    loadData()
  }, [loadData])

  const handleResolve = async () => {
    try {
      await resolveConflict({
        conflict_id: resolveConflictId,
        dir_path: dirPath || undefined,
        resolution: resolveResolution,
      })
      message.success('冲突已解决')
      setResolveModalVisible(false)
      loadData()
    } catch (error) {
      console.error('Failed to resolve conflict:', error)
      message.error('解决冲突失败')
    }
  }

  const handleAutoResolve = async () => {
    try {
      const result = await autoResolveConflicts({
        dir_path: dirPath || undefined,
        policy: autoResolvePolicy,
      })
      if (result.success) {
        message.success(`自动解决了 ${result.resolved_count} 个冲突`)
        setAutoResolveModalVisible(false)
        loadData()
      } else {
        message.error(`自动解决失败: ${result.error}`)
      }
    } catch (error) {
      console.error('Failed to auto-resolve:', error)
      message.error('自动解决失败')
    }
  }

  const handleBatchResolve = async () => {
    try {
      const result = await batchResolveConflicts({
        dir_path: dirPath || undefined,
        recursive: true,
        conflict_type: batchConflictType,
        policy: batchPolicy,
      })
      if (result.success) {
        message.success(`批量解决了 ${result.resolved_count} 个冲突`)
        setBatchModalVisible(false)
        loadData()
      } else {
        message.error(`批量解决失败: ${result.error}`)
      }
    } catch (error) {
      console.error('Failed to batch-resolve:', error)
      message.error('批量解决失败')
    }
  }

  const handleBatchIgnore = async () => {
    try {
      const result = await batchIgnoreConflicts({
        dir_path: dirPath || undefined,
        conflict_type: -1,
      })
      if (result.success) {
        message.success(`批量忽略了 ${result.ignored_count} 个冲突`)
        loadData()
      } else {
        message.error(`批量忽略失败: ${result.error}`)
      }
    } catch (error) {
      console.error('Failed to batch-ignore:', error)
      message.error('批量忽略失败')
    }
  }

  const columns = [
    {
      title: '冲突ID',
      dataIndex: 'id',
      key: 'id',
      width: 120,
      render: (id: string) => (
        <Tooltip title={id}>
          <span style={{ fontFamily: 'monospace' }}>{id.slice(0, 12)}...</span>
        </Tooltip>
      ),
    },
    {
      title: '类型',
      dataIndex: 'conflict_type',
      key: 'conflict_type',
      width: 130,
      render: (t: number) => {
        const info = CONFLICT_TYPE_LABELS[t] || { label: `Unknown(${t})`, color: 'default' }
        return <Tag color={info.color}>{info.label}</Tag>
      },
    },
    {
      title: '文件名',
      dataIndex: 'base_name',
      key: 'base_name',
      ellipsis: true,
    },
    {
      title: '目录路径',
      dataIndex: 'dir_path',
      key: 'dir_path',
      width: 200,
      ellipsis: true,
      render: (p: string) => p || '/',
    },
    {
      title: '分支数',
      key: 'branch_count',
      width: 80,
      render: (_: unknown, record: ConflictRecord) => record.branches?.length ?? 0,
    },
    {
      title: '创建时间',
      dataIndex: 'create_time',
      key: 'create_time',
      width: 160,
      render: formatTime,
    },
    {
      title: '状态',
      dataIndex: 'resolved',
      key: 'resolved',
      width: 100,
      render: (resolved: boolean, record: ConflictRecord) =>
        resolved ? (
          <Tag icon={<CheckCircleOutlined />} color="success">
            已解决
          </Tag>
        ) : (
          <Tooltip title={RESOLUTION_LABELS[record.resolution] || '未处理'}>
            <Tag icon={<WarningOutlined />} color="warning">
              待处理
            </Tag>
          </Tooltip>
        ),
    },
    {
      title: '解决时间',
      dataIndex: 'resolved_time',
      key: 'resolved_time',
      width: 160,
      render: formatTime,
    },
    {
      title: '操作',
      key: 'actions',
      width: 200,
      render: (_: unknown, record: ConflictRecord) => (
        <Space>
          <Button
            size="small"
            icon={<EyeOutlined />}
            onClick={() => {
              setCurrentConflict(record)
              setDetailVisible(true)
            }}
          >
            详情
          </Button>
          {!record.resolved && (
            <Button
              size="small"
              type="primary"
              onClick={() => {
                setResolveConflictId(record.id)
                setResolveResolution(0)
                setResolveModalVisible(true)
              }}
            >
              解决
            </Button>
          )}
        </Space>
      ),
    },
  ]

  return (
    <div>
      {/* Statistics Cards */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="总冲突数"
              value={stats?.total_count ?? 0}
              prefix={<WarningOutlined style={{ color: '#1890ff' }} />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="待处理"
              value={stats?.unresolved_count ?? 0}
              valueStyle={{ color: '#faad14' }}
              prefix={<WarningOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="已解决"
              value={stats?.resolved_count ?? 0}
              valueStyle={{ color: '#52c41a' }}
              prefix={<CheckCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="解决率"
              value={
                stats && stats.total_count > 0
                  ? ((stats.resolved_count / stats.total_count) * 100).toFixed(1)
                  : '0.0'
              }
              suffix="%"
            />
          </Card>
        </Col>
      </Row>

      {/* Conflict Type Breakdown */}
      {stats && stats.total_count > 0 && (
        <Card title="冲突类型分布" style={{ marginBottom: 16, borderRadius: 12 }}>
          <Row gutter={16}>
            {Object.entries(CONFLICT_TYPE_LABELS).map(([k, info]) => {
              const type = Number(k)
              const count =
                type === 0 ? stats.create_create_count :
                type === 1 ? stats.write_write_count :
                type === 2 ? stats.write_unlink_count :
                type === 3 ? stats.delete_create_count :
                type === 4 ? stats.rename_conflict_count : 0
              const resolved =
                type === 0 ? stats.create_create_resolved :
                type === 1 ? stats.write_write_resolved :
                type === 2 ? stats.write_unlink_resolved :
                type === 3 ? stats.delete_create_resolved :
                type === 4 ? stats.rename_conflict_resolved : 0
              if (count === 0) return null
              return (
                <Col span={4} key={k}>
                  <Statistic
                    title={<Tag color={info.color}>{info.label}</Tag>}
                    value={count}
                    suffix={`/ 已解决 ${resolved}`}
                  />
                </Col>
              )
            })}
          </Row>
        </Card>
      )}

      {/* Conflict List */}
      <Card
        title="冲突管理"
        style={{ borderRadius: 12 }}
        extra={
          <Space>
            <Input
              placeholder="目录路径 (默认根目录)"
              value={dirPath}
              onChange={(e) => setDirPath(e.target.value)}
              style={{ width: 220 }}
              allowClear
            />
            <Space>
              <span style={{ fontSize: 13 }}>仅未解决</span>
              <Switch checked={unresolvedOnly} onChange={setUnresolvedOnly} size="small" />
            </Space>
            <Button icon={<ReloadOutlined />} onClick={loadData} loading={loading}>
              刷新
            </Button>
            <Button
              type="primary"
              icon={<ThunderboltOutlined />}
              onClick={() => setAutoResolveModalVisible(true)}
            >
              自动解决
            </Button>
            <Button
              icon={<FilterOutlined />}
              onClick={() => setBatchModalVisible(true)}
            >
              批量解决
            </Button>
            <Popconfirm
              title="确定忽略所有未解决的冲突吗？"
              onConfirm={handleBatchIgnore}
              okText="确定"
              cancelText="取消"
            >
              <Button danger icon={<StopOutlined />}>
                批量忽略
              </Button>
            </Popconfirm>
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={conflicts}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 20, showSizeChanger: true }}
          size="small"
          scroll={{ x: 1200 }}
        />
      </Card>

      {/* Detail Modal */}
      <Modal
        title={`冲突详情 - ${currentConflict?.base_name ?? ''}`}
        open={detailVisible}
        onCancel={() => setDetailVisible(false)}
        footer={null}
        width={800}
      >
        {currentConflict && (
          <div>
            <Descriptions bordered column={2} size="small">
              <Descriptions.Item label="冲突ID" span={2}>
                <span style={{ fontFamily: 'monospace' }}>{currentConflict.id}</span>
              </Descriptions.Item>
              <Descriptions.Item label="类型">
                <Tag color={CONFLICT_TYPE_LABELS[currentConflict.conflict_type]?.color}>
                  {CONFLICT_TYPE_LABELS[currentConflict.conflict_type]?.label}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="状态">
                {currentConflict.resolved ? (
                  <Tag color="success">已解决</Tag>
                ) : (
                  <Tag color="warning">待处理</Tag>
                )}
              </Descriptions.Item>
              <Descriptions.Item label="目录路径">
                {currentConflict.dir_path || '/'}
              </Descriptions.Item>
              <Descriptions.Item label="目录Inode">
                {currentConflict.dir_ino}
              </Descriptions.Item>
              <Descriptions.Item label="创建时间">
                {formatTime(currentConflict.create_time)}
              </Descriptions.Item>
              <Descriptions.Item label="解决时间">
                {formatTime(currentConflict.resolved_time)}
              </Descriptions.Item>
              {currentConflict.resolved && (
                <Descriptions.Item label="解决方案" span={2}>
                  {RESOLUTION_LABELS[currentConflict.resolution]}
                </Descriptions.Item>
              )}
            </Descriptions>

            <h4 style={{ marginTop: 16, marginBottom: 8 }}>冲突分支 ({currentConflict.branches.length})</h4>
            <Table
              dataSource={currentConflict.branches}
              rowKey={(b) => `${b.client_id}-${b.seq}`}
              pagination={false}
              size="small"
              columns={[
                {
                  title: '文件名',
                  dataIndex: 'name',
                  key: 'name',
                },
                {
                  title: '客户端ID',
                  dataIndex: 'client_id',
                  key: 'client_id',
                  width: 100,
                },
                {
                  title: 'Seq',
                  dataIndex: 'seq',
                  key: 'seq',
                  width: 80,
                },
                {
                  title: 'Inode',
                  dataIndex: 'inode',
                  key: 'inode',
                  width: 100,
                },
                {
                  title: '大小',
                  dataIndex: 'size',
                  key: 'size',
                  width: 100,
                  render: formatSize,
                },
                {
                  title: '修改时间',
                  dataIndex: 'mtime',
                  key: 'mtime',
                  width: 160,
                  render: formatTime,
                },
                {
                  title: '类型',
                  dataIndex: 'file_type',
                  key: 'file_type',
                  width: 90,
                  render: (t: number) => {
                    if (t === 0) return <Tag>普通文件</Tag>
                    if (t === 1) return <Tag color="blue">目录</Tag>
                    if (t === 2) return <Tag color="purple">符号链接</Tag>
                    return <Tag>未知({t})</Tag>
                  },
                },
              ]}
            />

            {!currentConflict.resolved && (
              <div style={{ marginTop: 16, textAlign: 'right' }}>
                <Button
                  type="primary"
                  onClick={() => {
                    setResolveConflictId(currentConflict.id)
                    setResolveResolution(0)
                    setResolveModalVisible(true)
                    setDetailVisible(false)
                  }}
                >
                  解决此冲突
                </Button>
              </div>
            )}
          </div>
        )}
      </Modal>

      {/* Resolve Conflict Modal */}
      <Modal
        title="解决冲突"
        open={resolveModalVisible}
        onOk={handleResolve}
        onCancel={() => setResolveModalVisible(false)}
        okText="确定"
        cancelText="取消"
      >
        <div style={{ marginBottom: 8 }}>请选择解决方案：</div>
        <Select
          value={resolveResolution}
          onChange={setResolveResolution}
          style={{ width: '100%' }}
          options={Object.entries(RESOLUTION_LABELS).map(([k, v]) => ({
            value: Number(k),
            label: v,
          }))}
        />
      </Modal>

      {/* Auto Resolve Modal */}
      <Modal
        title="自动解决冲突"
        open={autoResolveModalVisible}
        onOk={handleAutoResolve}
        onCancel={() => setAutoResolveModalVisible(false)}
        okText="执行"
        cancelText="取消"
      >
        <div style={{ marginBottom: 8 }}>
          将对目录 <code>{dirPath || '/'}</code> 下所有未解决冲突应用所选策略：
        </div>
        <Select
          value={autoResolvePolicy}
          onChange={setAutoResolvePolicy}
          style={{ width: '100%' }}
          options={Object.entries(POLICY_LABELS).map(([k, v]) => ({
            value: Number(k),
            label: v,
          }))}
        />
      </Modal>

      {/* Batch Resolve Modal */}
      <Modal
        title="批量解决冲突"
        open={batchModalVisible}
        onOk={handleBatchResolve}
        onCancel={() => setBatchModalVisible(false)}
        okText="执行"
        cancelText="取消"
      >
        <div style={{ marginBottom: 8 }}>冲突类型：</div>
        <Select
          value={batchConflictType}
          onChange={setBatchConflictType}
          style={{ width: '100%', marginBottom: 12 }}
          options={[
            { value: -1, label: '全部类型' },
            ...Object.entries(CONFLICT_TYPE_LABELS).map(([k, v]) => ({
              value: Number(k),
              label: v.label,
            })),
          ]}
        />
        <div style={{ marginBottom: 8 }}>解决策略：</div>
        <Select
          value={batchPolicy}
          onChange={setBatchPolicy}
          style={{ width: '100%' }}
          options={Object.entries(POLICY_LABELS).map(([k, v]) => ({
            value: Number(k),
            label: v,
          }))}
        />
      </Modal>
    </div>
  )
}

export default Conflicts
