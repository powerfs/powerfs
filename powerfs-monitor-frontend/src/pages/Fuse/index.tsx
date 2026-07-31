import { useState, useEffect } from 'react'
import { Card, Table, Tag, Button, Modal, Form, Input, Space, Popconfirm, message, Tooltip, Typography, Descriptions, Drawer, Tabs, Statistic, Row, Col, Progress, Result } from 'antd'
import {
  FolderOpenOutlined,
  PlusOutlined,
  DeleteOutlined,
  ReloadOutlined,
  InfoCircleOutlined,
  BarChartOutlined,
  CloudServerOutlined,
  RocketOutlined,
} from '@ant-design/icons'
import type { FuseMount, ClientStats } from '@/types'
import { getFuseMounts, createFuseMount, deleteFuseMount, getFuseClientStats } from '@/services/api'

const { Text } = Typography

function formatBytes(bytes: number | undefined): string {
  if (!bytes) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function formatLatency(us: number | undefined): string {
  if (!us) return '-'
  if (us < 1000) return `${us} μs`
  return `${(us / 1000).toFixed(2)} ms`
}

function Fuse() {
  const [mounts, setMounts] = useState<FuseMount[]>([])
  const [createModalVisible, setCreateModalVisible] = useState(false)
  const [form] = Form.useForm()
  const [statsDrawerVisible, setStatsDrawerVisible] = useState(false)
  const [currentClient, setCurrentClient] = useState<FuseMount | null>(null)
  const [currentStats, setCurrentStats] = useState<ClientStats | null>(null)
  const [statsLoading, setStatsLoading] = useState(false)

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

  const handleViewStats = async (record: FuseMount) => {
    setCurrentClient(record)
    setCurrentStats(record.stats ?? null)
    setStatsDrawerVisible(true)
    // Always attempt to refresh latest stats from backend
    setStatsLoading(true)
    try {
      const fresh = await getFuseClientStats(record.id)
      if (fresh) {
        setCurrentStats(fresh)
      }
    } catch (error) {
      console.warn('Failed to fetch fresh stats:', error)
    } finally {
      setStatsLoading(false)
    }
  }

  const handleCreateMount = async () => {
    try {
      const values = await form.validateFields()
      await createFuseMount({
        mount_point: values.mount_point,
        collection: values.collection,
        replication: values.replication,
        filer_address: values.filer_address,
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
          <FolderOpenOutlined style={{ marginRight: 8, color: '#1890ff' }} />
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
      title: '队列深度',
      key: 'queue_depth',
      width: 110,
      render: (_: unknown, record: FuseMount) => {
        const s = record.stats
        if (!s) return <Text type="secondary">-</Text>
        return (
          <Tooltip title={`数据 ${s.data_queue_depth} / Lease ${s.lease_queue_depth} / 管理 ${s.admin_queue_depth}`}>
            <Tag color="blue">
              {s.data_queue_depth}/{s.lease_queue_depth}/{s.admin_queue_depth}
            </Tag>
          </Tooltip>
        )
      },
    },
    {
      title: '熔断器',
      key: 'circuit_breaker',
      width: 100,
      render: (_: unknown, record: FuseMount) => {
        const s = record.stats
        if (!s) return <Text type="secondary">-</Text>
        const color = s.cb_open_count > 0 ? 'red' : s.cb_half_open_count > 0 ? 'orange' : 'green'
        return (
          <Tooltip title={`关闭 ${s.cb_closed_count} / 开启 ${s.cb_open_count} / 半开 ${s.cb_half_open_count} (累计触发 ${s.cb_trip_total})`}>
            <Tag color={color}>
              {s.cb_closed_count}C / {s.cb_open_count}O / {s.cb_half_open_count}H
            </Tag>
          </Tooltip>
        )
      },
    },
    {
      title: 'Coalescer',
      key: 'coalescer',
      width: 100,
      render: (_: unknown, record: FuseMount) => {
        const s = record.stats
        if (!s) return <Text type="secondary">-</Text>
        return (
          <Tooltip title={`脏条目 ${s.coalescer_dirty_entries} / 写入 ${s.coalescer_writes_in_total} / 刷新 ${s.coalescer_flushes_out_total}`}>
            <Tag color={s.coalescer_dirty_bytes > 0 ? 'orange' : 'green'}>
              {formatBytes(s.coalescer_dirty_bytes)}
            </Tag>
          </Tooltip>
        )
      },
    },
    {
      title: '操作',
      key: 'actions',
      width: 180,
      render: (_: unknown, record: FuseMount) => (
        <Space>
          <Button size="small" onClick={() => handleViewStats(record)}>
            <BarChartOutlined /> 统计
          </Button>
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
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Text type="secondary" style={{ fontSize: 13 }}>
            FS（文件系统）管理展示客户端挂载到 PowerFS 的所有入口，包括用户态 FUSE 挂载和未来接入的内核态 VFS 挂载。
          </Text>
        </div>
      </Card>

      <Tabs
        defaultActiveKey="fuse"
        size="large"
        style={{ marginBottom: 16 }}
        items={[
          {
            key: 'fuse',
            label: (
              <span>
                <FolderOpenOutlined style={{ marginRight: 6 }} />
                FUSE 客户端
              </span>
            ),
            children: (
              <Card
                title="FUSE（用户态）挂载管理"
                style={{ borderRadius: 12 }}
                bodyStyle={{ padding: '20px' }}
                extra={
                  <Space>
                    <Tooltip title="刷新">
                      <Button icon={<ReloadOutlined />} onClick={loadMounts}>刷新</Button>
                    </Tooltip>
                    <Button type="primary" onClick={() => setCreateModalVisible(true)}>
                      <PlusOutlined /> 新建挂载
                    </Button>
                  </Space>
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
            ),
          },
          {
            key: 'kernel',
            label: (
              <span>
                <RocketOutlined style={{ marginRight: 6 }} />
                内核文件系统
                <Tag color="blue" style={{ marginLeft: 8, fontSize: 10 }}>待接入</Tag>
              </span>
            ),
            children: (
              <Card
                style={{ borderRadius: 12 }}
                bodyStyle={{ padding: '20px' }}
                title="内核 VFS 挂载管理"
              >
                <Result
                  icon={<CloudServerOutlined />}
                  title="内核态挂载接入中"
                  subTitle="后续将通过读取节点 /proc/mounts 或 mountinfo 汇总 ext4/xfs 等本地文件系统挂载，以及 PowerFS 内核模块（VFS over FUSE）的挂载信息。"
                />
                <Descriptions column={1} size="small" style={{ marginTop: 24 }}>
                  <Descriptions.Item label="规划内容">
                    <ul style={{ margin: 0, paddingLeft: 20 }}>
                      <li>各节点的本地文件系统挂载列表（ext4 / xfs / btrfs 等）</li>
                      <li>挂载点、设备、文件系统类型、可用空间、使用率</li>
                      <li>只读 / 读写属性、挂载参数</li>
                      <li>PowerFS 内核客户端（若后续支持）的挂载状态</li>
                    </ul>
                  </Descriptions.Item>
                </Descriptions>
              </Card>
            ),
          },
        ]}
      />

      <Card title="常见问题" size="small" style={{ marginTop: 24 }}>
        <Descriptions column={1} size="small">
          <Descriptions.Item label="什么是 FUSE？">
            FUSE（Filesystem in Userspace）是一种在用户空间实现文件系统的技术。PowerFS 通过 FUSE 允许用户将分布式文件系统挂载为本地文件系统。
          </Descriptions.Item>
          <Descriptions.Item label="什么是 Collection？">
            Collection 是 PowerFS 中的数据集合概念，类似于逻辑卷或文件系统分区。不同 Collection 之间的数据是隔离的。
          </Descriptions.Item>
          <Descriptions.Item label="什么是脏 Chunks？">
            脏 Chunks 是指已经写入但尚未持久化到后端存储的数据块。这些数据存储在客户端缓存中，定期会被刷新到后端。
          </Descriptions.Item>
          <Descriptions.Item label="副本策略是什么？">
            副本策略决定了数据在集群中的存储方式。例如 "000" 表示不使用纠删码，仅使用副本；"101" 表示 1 个数据分片、0 个校验分片、1 个副本。
          </Descriptions.Item>
        </Descriptions>
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
            name="filer_address"
            label="Filer地址"
            rules={[{ required: true, message: '请输入Filer节点地址' }]}
          >
            <Input placeholder="localhost:8888" />
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

      <Drawer
        title={
          currentClient ? (
            <Space>
              <BarChartOutlined />
              <span>客户端统计 - {currentClient.id.slice(0, 8)}</span>
            </Space>
          ) : '客户端统计'
        }
        placement="right"
        width={720}
        open={statsDrawerVisible}
        onClose={() => setStatsDrawerVisible(false)}
        loading={statsLoading}
        extra={
          currentClient ? (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {currentClient.host ?? 'unknown'} · {currentClient.mount_point}
            </Text>
          ) : null
        }
      >
        {currentStats ? (
          <Tabs
            defaultActiveKey="overview"
            items={[
              {
                key: 'overview',
                label: '概览',
                children: (
                  <div>
                    <Row gutter={16}>
                      <Col span={12}>
                        <Statistic
                          title="读延迟 p50"
                          value={formatLatency(currentStats.read_latency_p50_us)}
                        />
                      </Col>
                      <Col span={12}>
                        <Statistic
                          title="读延迟 p99"
                          value={formatLatency(currentStats.read_latency_p99_us)}
                          valueStyle={currentStats.read_latency_p99_us > 100000 ? { color: '#cf1322' } : undefined}
                        />
                      </Col>
                    </Row>
                    <Row gutter={16} style={{ marginTop: 16 }}>
                      <Col span={12}>
                        <Statistic
                          title="写延迟 p50"
                          value={formatLatency(currentStats.write_latency_p50_us)}
                        />
                      </Col>
                      <Col span={12}>
                        <Statistic
                          title="写延迟 p99"
                          value={formatLatency(currentStats.write_latency_p99_us)}
                          valueStyle={currentStats.write_latency_p99_us > 100000 ? { color: '#cf1322' } : undefined}
                        />
                      </Col>
                    </Row>
                    <Card size="small" title="活跃 Leases" style={{ marginTop: 16 }}>
                      <Row gutter={16}>
                        <Col span={8}>
                          <Statistic title="活跃数" value={currentStats.active_leases} />
                        </Col>
                        <Col span={8}>
                          <Statistic title="续租次数" value={currentStats.lease_renewals_total} />
                        </Col>
                        <Col span={8}>
                          <Statistic
                            title="过期数"
                            value={currentStats.lease_expired_total}
                            valueStyle={currentStats.lease_expired_total > 0 ? { color: '#cf1322' } : undefined}
                          />
                        </Col>
                      </Row>
                    </Card>
                  </div>
                ),
              },
              {
                key: 'scheduler',
                label: '多队列调度',
                children: (
                  <div>
                    <Descriptions column={1} size="small" bordered>
                      <Descriptions.Item label="数据队列深度">
                        {currentStats.data_queue_depth}
                      </Descriptions.Item>
                      <Descriptions.Item label="Lease 队列深度">
                        {currentStats.lease_queue_depth}
                      </Descriptions.Item>
                      <Descriptions.Item label="管理队列深度">
                        {currentStats.admin_queue_depth}
                      </Descriptions.Item>
                    </Descriptions>
                    <Row gutter={16} style={{ marginTop: 16 }}>
                      <Col span={8}>
                        <Statistic title="数据请求累计" value={currentStats.data_processed_total} />
                      </Col>
                      <Col span={8}>
                        <Statistic title="Lease 请求累计" value={currentStats.lease_processed_total} />
                      </Col>
                      <Col span={8}>
                        <Statistic title="管理请求累计" value={currentStats.admin_processed_total} />
                      </Col>
                    </Row>
                  </div>
                ),
              },
              {
                key: 'circuit_breaker',
                label: '熔断器',
                children: (
                  <div>
                    <Row gutter={16}>
                      <Col span={8}>
                        <Card size="small">
                          <Statistic
                            title="关闭 (Closed)"
                            value={currentStats.cb_closed_count}
                            valueStyle={{ color: '#3f8600' }}
                          />
                        </Card>
                      </Col>
                      <Col span={8}>
                        <Card size="small">
                          <Statistic
                            title="开启 (Open)"
                            value={currentStats.cb_open_count}
                            valueStyle={currentStats.cb_open_count > 0 ? { color: '#cf1322' } : undefined}
                          />
                        </Card>
                      </Col>
                      <Col span={8}>
                        <Card size="small">
                          <Statistic
                            title="半开 (Half-Open)"
                            value={currentStats.cb_half_open_count}
                            valueStyle={currentStats.cb_half_open_count > 0 ? { color: '#fa8c16' } : undefined}
                          />
                        </Card>
                      </Col>
                    </Row>
                    <Card size="small" title="累计触发" style={{ marginTop: 16 }}>
                      <Statistic
                        value={currentStats.cb_trip_total}
                        valueStyle={currentStats.cb_trip_total > 0 ? { color: '#cf1322' } : undefined}
                      />
                    </Card>
                  </div>
                ),
              },
              {
                key: 'coalescer',
                label: '写合并',
                children: (
                  <div>
                    <Card size="small" title="脏数据">
                      <Statistic
                        title="脏字节"
                        value={formatBytes(currentStats.coalescer_dirty_bytes)}
                      />
                      <Progress
                        percent={Math.min(100, (currentStats.coalescer_dirty_bytes / (64 * 1024 * 1024)) * 100)}
                        size="small"
                        status="active"
                        style={{ marginTop: 8 }}
                      />
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        相对 64MB 刷新阈值的占比
                      </Text>
                    </Card>
                    <Row gutter={16} style={{ marginTop: 16 }}>
                      <Col span={8}>
                        <Statistic title="脏条目数" value={currentStats.coalescer_dirty_entries} />
                      </Col>
                      <Col span={8}>
                        <Statistic title="写入累计" value={currentStats.coalescer_writes_in_total} />
                      </Col>
                      <Col span={8}>
                        <Statistic title="刷新累计" value={currentStats.coalescer_flushes_out_total} />
                      </Col>
                    </Row>
                    {currentStats.coalescer_writes_in_total > 0 && (
                      <Card size="small" title="合并率" style={{ marginTop: 16 }}>
                        <Progress
                          percent={Math.round(
                            (1 - currentStats.coalescer_flushes_out_total / currentStats.coalescer_writes_in_total) * 100,
                          )}
                          status="success"
                        />
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          合并率 = 1 - 刷新次数 / 写入次数
                        </Text>
                      </Card>
                    )}
                  </div>
                ),
              },
              {
                key: 'pool',
                label: '连接池',
                children: (
                  <div>
                    <Row gutter={16}>
                      <Col span={8}>
                        <Statistic title="活跃连接" value={currentStats.pool_active_connections} />
                      </Col>
                      <Col span={8}>
                        <Statistic
                          title="重连次数"
                          value={currentStats.pool_reconnect_total}
                          valueStyle={currentStats.pool_reconnect_total > 0 ? { color: '#fa8c16' } : undefined}
                        />
                      </Col>
                      <Col span={8}>
                        <Statistic
                          title="Ping 失败"
                          value={currentStats.pool_ping_failures}
                          valueStyle={currentStats.pool_ping_failures > 0 ? { color: '#cf1322' } : undefined}
                        />
                      </Col>
                    </Row>
                  </div>
                ),
              },
            ]}
          />
        ) : (
          <div style={{ textAlign: 'center', padding: '40px 0' }}>
            <Text type="secondary">暂无统计数据</Text>
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                请确认 FUSE 客户端已启动并连接至 Master
              </Text>
            </div>
          </div>
        )}
      </Drawer>
    </div>
  )
}

export default Fuse