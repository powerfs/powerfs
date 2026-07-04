import { useEffect, useState } from 'react'
import { Card, Table, Tag, Button, Modal, Space, Tabs, message, Switch } from 'antd'
import {
  BellOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined,
  EyeOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  LeftCircleOutlined,
  CheckCircleOutlined as AcknowledgeOutlined,
} from '@ant-design/icons'
import type { AlertInfo, AlertRule } from '@/types'
import { getAlerts, getAlertRules, acknowledgeAlert } from '@/services/api'

function Alerts() {
  const [alerts, setAlerts] = useState<AlertInfo[]>([])
  const [rules, setRules] = useState<AlertRule[]>([])
  const [selectedAlert, setSelectedAlert] = useState<AlertInfo | null>(null)
  const [showDetail, setShowDetail] = useState(false)
  const [showRuleForm, setShowRuleForm] = useState(false)
  const [editingRule, setEditingRule] = useState<AlertRule | null>(null)

  useEffect(() => {
    loadData()
    const interval = setInterval(loadData, 10000)
    return () => clearInterval(interval)
  }, [])

  const loadData = async () => {
    const [alertList, ruleList] = await Promise.all([
      getAlerts(),
      getAlertRules(),
    ])
    setAlerts(alertList)
    setRules(ruleList)
  }

  const handleViewDetail = (alert: AlertInfo) => {
    setSelectedAlert(alert)
    setShowDetail(true)
  }

  const handleAcknowledge = async (alert: AlertInfo) => {
    await acknowledgeAlert(alert.id)
    message.success('告警已确认')
    loadData()
  }

  const toggleRule = (rule: AlertRule) => {
    message.success(rule.enabled ? '规则已禁用' : '规则已启用')
    setRules(rules.map(r => r.id === rule.id ? { ...r, enabled: !r.enabled } : r))
  }

  const alertColumns = [
    {
      title: '告警名称',
      dataIndex: 'name',
      key: 'name',
      width: 200,
    },
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
      width: 100,
      render: (severity: string) => {
        const config = {
          critical: { color: 'red', icon: <LeftCircleOutlined />, text: '严重' },
          warning: { color: 'orange', icon: <ClockCircleOutlined />, text: '警告' },
          info: { color: 'blue', icon: <InfoCircleOutlined />, text: '信息' },
        }
        const { color, icon, text } = config[severity as keyof typeof config]
        return (
          <Tag color={color}>
            {icon} {text}
          </Tag>
        )
      },
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => {
        const config = {
          firing: { color: 'red', icon: <LeftCircleOutlined />, text: '触发中' },
          pending: { color: 'orange', icon: <ClockCircleOutlined />, text: '待确认' },
          resolved: { color: 'green', icon: <CheckCircleOutlined />, text: '已恢复' },
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
      title: '来源',
      dataIndex: 'source',
      key: 'source',
      width: 120,
    },
    {
      title: '消息',
      dataIndex: 'message',
      key: 'message',
      ellipsis: true,
    },
    {
      title: '触发时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (time: string) => new Date(time).toLocaleString(),
    },
    {
      title: '操作',
      key: 'action',
      width: 150,
      render: (_: unknown, record: AlertInfo) => (
        <Space>
          <Button
            type="text"
            icon={<EyeOutlined />}
            onClick={() => handleViewDetail(record)}
          >
            详情
          </Button>
          {record.status === 'firing' && (
            <Button
              type="text"
              icon={<AcknowledgeOutlined />}
              onClick={() => handleAcknowledge(record)}
            >
              确认
            </Button>
          )}
        </Space>
      ),
    },
  ]

  const ruleColumns = [
    {
      title: '规则名称',
      dataIndex: 'name',
      key: 'name',
      width: 200,
    },
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
      width: 100,
      render: (severity: string) => {
        const colors: Record<string, string> = {
          critical: 'red',
          warning: 'orange',
          info: 'blue',
        }
        const texts: Record<string, string> = {
          critical: '严重',
          warning: '警告',
          info: '信息',
        }
        return <Tag color={colors[severity]}>{texts[severity]}</Tag>
      },
    },
    {
      title: '条件',
      key: 'condition',
      width: 300,
      render: (_: unknown, record: AlertRule) => (
        <span>
          {record.condition.metric} {record.condition.operator} {record.condition.value}
        </span>
      ),
    },
    {
      title: '启用',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 80,
      render: (enabled: boolean, record: AlertRule) => (
        <Switch
          checked={enabled}
          onChange={() => toggleRule(record)}
        />
      ),
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
      width: 150,
      render: (_: unknown, record: AlertRule) => (
        <Space>
          <Button
            type="text"
            icon={<EditOutlined />}
            onClick={() => {
              setEditingRule(record)
              setShowRuleForm(true)
            }}
          >
            编辑
          </Button>
          <Button
            type="text"
            danger
            icon={<DeleteOutlined />}
          >
            删除
          </Button>
        </Space>
      ),
    },
  ]

  const firingCount = alerts.filter(a => a.status === 'firing').length
  const criticalCount = alerts.filter(a => a.status === 'firing' && a.severity === 'critical').length
  const warningCount = alerts.filter(a => a.status === 'firing' && a.severity === 'warning').length

  return (
    <div>
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#fff2f0', padding: 8, borderRadius: 8 }}>
                  <BellOutlined style={{ fontSize: 24, color: '#f5222d' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>触发中告警</span>
              </div>
              <span style={{ fontSize: 32, fontWeight: 'bold', color: '#f5222d' }}>
                {firingCount}
              </span>
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#fff2f0', padding: 8, borderRadius: 8 }}>
                  <LeftCircleOutlined style={{ fontSize: 24, color: '#f5222d' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>严重告警</span>
              </div>
              <span style={{ fontSize: 32, fontWeight: 'bold', color: '#f5222d' }}>
                {criticalCount}
              </span>
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#fff7e6', padding: 8, borderRadius: 8 }}>
                  <ClockCircleOutlined style={{ fontSize: 24, color: '#faad14' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>警告告警</span>
              </div>
              <span style={{ fontSize: 32, fontWeight: 'bold', color: '#faad14' }}>
                {warningCount}
              </span>
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#f6ffed', padding: 8, borderRadius: 8 }}>
                  <CheckCircleOutlined style={{ fontSize: 24, color: '#52c41a' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>已恢复</span>
              </div>
              <span style={{ fontSize: 32, fontWeight: 'bold', color: '#52c41a' }}>
                {alerts.filter(a => a.status === 'resolved').length}
              </span>
            </Space>
          </Card>
        </Col>
      </Row>

      <Card style={{ borderRadius: 12 }}>
        <Tabs
          defaultActiveKey="alerts"
          items={[
            {
              key: 'alerts',
              label: (
                <span>
                  <BellOutlined /> 告警列表
                </span>
              ),
              children: (
                <Table
                  columns={alertColumns}
                  dataSource={alerts}
                  rowKey="id"
                  pagination={{ pageSize: 10 }}
                  scroll={{ x: 1200 }}
                />
              ),
            },
            {
              key: 'rules',
              label: (
                <span>
                  <LeftCircleOutlined /> 告警规则
                </span>
              ),
              children: (
                <div>
                  <Button
                    type="primary"
                    icon={<PlusOutlined />}
                    style={{ marginBottom: 16 }}
                    onClick={() => {
                      setEditingRule(null)
                      setShowRuleForm(true)
                    }}
                  >
                    添加规则
                  </Button>
                  <Table
                    columns={ruleColumns}
                    dataSource={rules}
                    rowKey="id"
                    pagination={{ pageSize: 10 }}
                    scroll={{ x: 1000 }}
                  />
                </div>
              ),
            },
          ]}
        />
      </Card>

      <Modal
        title="告警详情"
        open={showDetail}
        onCancel={() => setShowDetail(false)}
        footer={null}
        width={500}
      >
        {selectedAlert && (
          <Space direction="vertical" style={{ width: '100%', gap: 20 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ background: '#fff2f0', padding: 12, borderRadius: 12 }}>
                <LeftCircleOutlined style={{ fontSize: 32, color: '#f5222d' }} />
              </div>
              <div>
                <h3 style={{ margin: 0 }}>{selectedAlert.name}</h3>
                <Tag color={selectedAlert.severity === 'critical' ? 'red' : selectedAlert.severity === 'warning' ? 'orange' : 'blue'}>
                  {selectedAlert.severity === 'critical' ? '严重' : selectedAlert.severity === 'warning' ? '警告' : '信息'}
                </Tag>
              </div>
            </div>

            <div>
              <h4 style={{ margin: '0 0 8px' }}>告警消息</h4>
              <p style={{ padding: '12px', background: '#f5f5f5', borderRadius: 8 }}>
                {selectedAlert.message}
              </p>
            </div>

            <div>
              <h4 style={{ margin: '0 0 12px' }}>详细信息</h4>
              <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>来源</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{selectedAlert.source}</p>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>状态</span>
                  <Tag color={selectedAlert.status === 'firing' ? 'red' : selectedAlert.status === 'pending' ? 'orange' : 'green'}>
                    {selectedAlert.status === 'firing' ? '触发中' : selectedAlert.status === 'pending' ? '待确认' : '已恢复'}
                  </Tag>
                </div>
                <div>
                  <span style={{ color: '#8c8c8c', fontSize: 12 }}>触发时间</span>
                  <p style={{ margin: '4px 0', fontWeight: 500 }}>{new Date(selectedAlert.created_at).toLocaleString()}</p>
                </div>
                {selectedAlert.resolved_at && (
                  <div>
                    <span style={{ color: '#8c8c8c', fontSize: 12 }}>恢复时间</span>
                    <p style={{ margin: '4px 0', fontWeight: 500 }}>{new Date(selectedAlert.resolved_at).toLocaleString()}</p>
                  </div>
                )}
              </div>
            </div>
          </Space>
        )}
      </Modal>

      <Modal
        title={editingRule ? '编辑规则' : '添加规则'}
        open={showRuleForm}
        onCancel={() => setShowRuleForm(false)}
        footer={null}
        width={600}
      >
        <Space direction="vertical" style={{ width: '100%', gap: 20 }}>
          <div>
            <label style={{ display: 'block', marginBottom: 8, fontWeight: 500 }}>规则名称</label>
            <input
              type="text"
              defaultValue={editingRule?.name || ''}
              style={{ width: '100%', padding: '8px 12px', borderRadius: 6, border: '1px solid #d9d9d9' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: 8, fontWeight: 500 }}>描述</label>
            <textarea
              defaultValue={editingRule?.description || ''}
              style={{ width: '100%', padding: '8px 12px', borderRadius: 6, border: '1px solid #d9d9d9', height: 80 }}
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: 8, fontWeight: 500 }}>告警级别</label>
            <Select
              defaultValue={editingRule?.severity || 'warning'}
              style={{ width: '100%' }}
              options={[
                { value: 'critical', label: '严重' },
                { value: 'warning', label: '警告' },
                { value: 'info', label: '信息' },
              ]}
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: 8, fontWeight: 500 }}>监控指标</label>
            <Select
              defaultValue={editingRule?.condition.metric || ''}
              style={{ width: '100%' }}
              options={[
                { value: 'powerfs_node_cpu_usage', label: '节点CPU使用率' },
                { value: 'powerfs_node_mem_usage', label: '节点内存使用率' },
                { value: 'powerfs_node_disk_usage', label: '节点磁盘使用率' },
                { value: 'powerfs_kv_hit_ratio', label: 'KV命中率' },
                { value: 'powerfs_kv_memory_used', label: 'KV内存使用' },
              ]}
            />
          </div>
          <div style={{ display: 'flex', gap: 12 }}>
            <div style={{ flex: 1 }}>
              <label style={{ display: 'block', marginBottom: 8, fontWeight: 500 }}>操作符</label>
              <Select
                defaultValue={editingRule?.condition.operator || '>'}
                style={{ width: '100%' }}
                options={[
                  { value: '>', label: '大于' },
                  { value: '<', label: '小于' },
                  { value: '>=', label: '大于等于' },
                  { value: '<=', label: '小于等于' },
                ]}
              />
            </div>
            <div style={{ flex: 1 }}>
              <label style={{ display: 'block', marginBottom: 8, fontWeight: 500 }}>阈值</label>
              <input
                type="number"
                defaultValue={editingRule?.condition.value || ''}
                style={{ width: '100%', padding: '8px 12px', borderRadius: 6, border: '1px solid #d9d9d9' }}
              />
            </div>
            <div style={{ flex: 1 }}>
              <label style={{ display: 'block', marginBottom: 8, fontWeight: 500 }}>持续时间(秒)</label>
              <input
                type="number"
                defaultValue={editingRule?.condition.duration || 300}
                style={{ width: '100%', padding: '8px 12px', borderRadius: 6, border: '1px solid #d9d9d9' }}
              />
            </div>
          </div>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
            <Button onClick={() => setShowRuleForm(false)}>取消</Button>
            <Button type="primary" onClick={() => {
              message.success(editingRule ? '规则更新成功' : '规则创建成功')
              setShowRuleForm(false)
            }}>
              {editingRule ? '更新规则' : '创建规则'}
            </Button>
          </div>
        </Space>
      </Modal>
    </div>
  )
}

import { Row, Col, Select } from 'antd'
export default Alerts