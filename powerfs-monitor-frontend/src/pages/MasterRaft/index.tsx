import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Modal,
  Popconfirm,
  Progress,
  Row,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd'
import {
  ApiOutlined,
  CheckCircleFilled,
  CloudServerOutlined,
  CrownOutlined,
  ReloadOutlined,
  SwapOutlined,
  TeamOutlined,
  WarningFilled,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { MasterStatus, NodeInfo } from '@/types'
import { getMasterStatus, transferLeader } from '@/services/api'
import { useMetricStream } from '@/hooks/useMetricStream'

const { Title, Text } = Typography

function MasterRaft() {
  const { t } = useTranslation(['common', 'nav'])
  const [status, setStatus] = useState<MasterStatus | null>(null)
  const [loading, setLoading] = useState(true)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const s = await getMasterStatus()
      setStatus(s)
    } catch (e) {
      console.error(e)
      message.error('Failed to load Master Raft status')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
    // Backoff polling to 15s — WS delivers live node status; refresh here
    // only recomputes leader/raft_term aggregate.
    const iv = setInterval(() => void load(), 15000)
    return () => clearInterval(iv)
  }, [load])

  // Live patch: when WS pushes a master node update, merge it into the
  // status.nodes list and recompute leader/term if needed.
  useMetricStream({
    source: 'nodes',
    onMetricUpdate: (u) => {
      const payload = u.payload
      const patchOne = (node: NodeInfo) => {
        setStatus((prev) => {
          if (!prev) return prev
          const nodes = prev.nodes.map((n) =>
            n.id === node.id ? { ...n, ...node } : n,
          )
          const leader = nodes.find((n) => n.is_leader) ?? null
          return {
            ...prev,
            nodes,
            leader,
            raft_term: leader?.raft_term ?? prev.raft_term,
            // A follower is a healthy Raft participant — see backend
            // get_master_status in powerfs-monitor/src/main.rs.
            healthy_masters: nodes.filter((n) =>
              ['online', 'healthy', 'leader', 'follower'].includes(n.status),
            ).length,
          }
        })
      }
      if (Array.isArray(payload)) {
        ;(payload as NodeInfo[]).forEach(patchOne)
      } else if (payload && typeof payload === 'object') {
        patchOne(payload as NodeInfo)
      }
    },
  })

  const doTransferLeader = async (node: NodeInfo) => {
    // node.id 可能是 "1" / "master-1" 等，只要 parse 得到数字；否则尝试直接用纯数字部分
    const numeric = (() => {
      const n = Number(node.id)
      if (Number.isFinite(n) && Number.isInteger(n)) return n
      const m = /(\d+)/.exec(node.id)
      return m ? parseInt(m[1], 10) : NaN
    })()
    if (!Number.isFinite(numeric) || !Number.isInteger(numeric)) {
      message.error(`Invalid master node id: ${node.id}`)
      return
    }
    try {
      await transferLeader(numeric)
      message.success(`Leader transfer requested to ${node.id}`)
      setTimeout(load, 3000)
    } catch (e: any) {
      const detail = e?.response?.data?.error ?? e?.message ?? 'unknown error'
      message.error(`Leader transfer failed: ${detail}`)
    }
  }

  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 160,
      render: (id: string, r: NodeInfo) => (
        <Space>
          {r.is_leader ? (
            <CrownOutlined style={{ color: '#d4b106' }} />
          ) : (
            <TeamOutlined style={{ color: '#1677ff' }} />
          )}
          <Text strong>{id}</Text>
        </Space>
      ),
    },
    {
      title: t('common:role'),
      key: 'role',
      width: 120,
      render: (_: unknown, r: NodeInfo) =>
        r.is_leader ? (
          <Tag color="gold" icon={<CrownOutlined />}>{t('common:leader')}</Tag>
        ) : (
          <Tag color="blue">{t('common:follower')}</Tag>
        ),
    },
    {
      title: t('common:address'),
      key: 'addr',
      render: (_: unknown, r: NodeInfo) => (
        <Text type="secondary" className="font-mono" style={{ fontSize: 12 }}>
          {r.address}:{r.grpc_port} / HTTP {r.http_port}
        </Text>
      ),
    },
    {
      title: t('common:status'),
      dataIndex: 'status',
      key: 'status',
      width: 120,
      render: (s: string) => {
        const ok = ['online', 'healthy', 'leader', 'follower'].includes(s)
        return ok ? (
          <Tag icon={<CheckCircleFilled style={{ color: '#52c41a' }} />} color="success">
            {s}
          </Tag>
        ) : (
          <Tag icon={<WarningFilled style={{ color: '#faad14' }} />} color="warning">
            {s}
          </Tag>
        )
      },
    },
    {
      title: t('common:cpu'),
      key: 'cpu',
      width: 180,
      render: (_: unknown, r: NodeInfo) => <ResourceBar value={r.cpu_usage} />,
    },
    {
      title: t('common:memory'),
      key: 'mem',
      width: 180,
      render: (_: unknown, r: NodeInfo) => <ResourceBar value={r.mem_usage} />,
    },
    {
      title: 'Raft Term',
      dataIndex: 'raft_term',
      key: 'raft_term',
      width: 100,
      render: (v?: number) => (v !== undefined ? <span className="tabular-nums">{v}</span> : <Text type="secondary">-</Text>),
    },
    {
      title: t('common:operation'),
      key: 'action',
      width: 180,
      fixed: 'right' as const,
      render: (_: unknown, r: NodeInfo) => {
        if (r.is_leader) {
          return (
            <Tooltip title="Current leader">
              <Tag color="gold" icon={<CrownOutlined />}>{t('common:leader')}</Tag>
            </Tooltip>
          )
        }
        return (
          <Popconfirm
            title={t('common:transferConfirm', { target: r.id })}
            okText={t('common:confirm')}
            cancelText={t('common:cancel')}
            okButtonProps={{ danger: true }}
            onConfirm={() => doTransferLeader(r)}
          >
            <Button size="small" type="primary" ghost icon={<SwapOutlined />} danger>
              {t('common:transferLeader')}
            </Button>
          </Popconfirm>
        )
      },
    },
  ]

  const quorum = useMemo(() => (status ? Math.floor(status.total_masters / 2) + 1 : 0), [status])

  return (
    <div style={{ padding: 24 }}>
      <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            <ApiOutlined style={{ marginRight: 8 }} />
            {t('nav:items.masterRaft')}
          </Title>
          <Text type="secondary">
            Master Raft group status and leadership transfer operations (admin-only).
          </Text>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={() => void load()} loading={loading}>
            {t('common:refresh')}
          </Button>
        </Col>
      </Row>

      {status && (
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="Total Masters"
                value={status.total_masters}
                prefix={<CloudServerOutlined />}
                valueStyle={{ color: '#1677ff' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="Healthy / Quorum"
                value={`${status.healthy_masters} / ${quorum}`}
                prefix={<CheckCircleFilled />}
                valueStyle={{ color: status.healthy_masters >= quorum ? '#52c41a' : '#ff4d4f' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="Current Leader"
                value={status.leader?.id ?? '(none)'}
                prefix={<CrownOutlined />}
                valueStyle={{ color: status.leader ? '#d4b106' : '#ff4d4f' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="Raft Term"
                value={status.raft_term}
                prefix={<ApiOutlined />}
                valueStyle={{ color: '#722ed1' }}
              />
            </Card>
          </Col>
        </Row>
      )}

      {status && status.healthy_masters < quorum && (
        <Alert
          showIcon
          type="error"
          style={{ marginBottom: 16 }}
          message="Raft quorum lost"
          description={`Only ${status.healthy_masters}/${status.total_masters} masters healthy; quorum requires ${quorum}. Metadata writes may be unavailable.`}
        />
      )}

      <Card
        title={
          <Space>
            <CloudServerOutlined />
            Master Nodes
          </Space>
        }
      >
        <Table
          loading={loading}
          columns={columns}
          dataSource={status?.nodes ?? []}
          rowKey="id"
          pagination={false}
          scroll={{ x: 1200 }}
        />
      </Card>

      {status && status.leader && (
        <Card
          title={
            <Space style={{ marginTop: 16 }}>
              <CrownOutlined />
              Leader Details
            </Space>
          }
          style={{ marginTop: 16 }}
        >
          <Descriptions column={2} size="small" bordered>
            <Descriptions.Item label="ID">{status.leader.id}</Descriptions.Item>
            <Descriptions.Item label={t('common:address')}>
              {status.leader.address}:{status.leader.grpc_port}
            </Descriptions.Item>
            <Descriptions.Item label={t('common:status')}>{status.leader.status}</Descriptions.Item>
            <Descriptions.Item label="Raft Term">{status.leader.raft_term}</Descriptions.Item>
            <Descriptions.Item label={t('common:cpu')}>
              <ResourceBar value={status.leader.cpu_usage} />
            </Descriptions.Item>
            <Descriptions.Item label={t('common:memory')}>
              <ResourceBar value={status.leader.mem_usage} />
            </Descriptions.Item>
          </Descriptions>
        </Card>
      )}

      {/* Hidden re-confirm placeholder for future upgrade (currently uses Popconfirm inline) */}
      <HiddenDialog />
    </div>
  )
}

function ResourceBar({ value }: { value: number }) {
  const color = value > 80 ? '#ff4d4f' : value > 60 ? '#faad14' : '#52c41a'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <Progress percent={Math.min(100, value)} size="small" showInfo={false} strokeColor={color} style={{ flex: 1 }} />
      <span className="tabular-nums" style={{ fontSize: 12, minWidth: 42, textAlign: 'right' }}>
        {value.toFixed(0)}%
      </span>
    </div>
  )
}

function HiddenDialog() {
  const [open, setOpen] = useState(false)
  return (
    <Modal
      open={open}
      onCancel={() => setOpen(false)}
      onOk={() => setOpen(false)}
      style={{ display: 'none' }}
    />
  )
}

export default MasterRaft
