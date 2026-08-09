import { useCallback, useEffect, useState } from 'react'
import {
  Button,
  Card,
  Col,
  Form,
  InputNumber,
  Row,
  Space,
  Switch,
  Tabs,
  Typography,
  message,
} from 'antd'
import {
  ApiOutlined,
  BulbFilled,
  ReloadOutlined,
  SaveOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { CircuitBreakerConfig, CoalescerConfig } from '@/types'
import {
  getCircuitBreakerConfig,
  getCoalescerConfig,
  putCircuitBreakerConfig,
  putCoalescerConfig,
} from '@/services/api'

const { Title, Text } = Typography

function RuntimeConfig() {
  const { t } = useTranslation(['common', 'nav'])
  const [cb, setCb] = useState<CircuitBreakerConfig | null>(null)
  const [coa, setCoa] = useState<CoalescerConfig | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState<null | 'cb' | 'coa'>(null)
  const [cbForm] = Form.useForm()
  const [coaForm] = Form.useForm()

  const loadAll = useCallback(async () => {
    setLoading(true)
    try {
      const [c, k] = await Promise.all([getCircuitBreakerConfig(), getCoalescerConfig()])
      setCb(c)
      setCoa(k)
      cbForm.setFieldsValue(c)
      coaForm.setFieldsValue(k)
    } catch (e) {
      console.error(e)
      message.error('Failed to load runtime config')
    } finally {
      setLoading(false)
    }
  }, [cbForm, coaForm])

  useEffect(() => {
    void loadAll()
  }, [loadAll])

  const saveCb = async () => {
    if (!cb) return
    try {
      setSaving('cb')
      const payload = await cbForm.validateFields() as CircuitBreakerConfig
      await putCircuitBreakerConfig(payload)
      setCb(payload)
      message.success(`${t('common:circuitBreaker')} ${t('common:success')}`)
    } catch (e) {
      console.error(e)
      message.error('Failed to update Circuit Breaker config')
    } finally {
      setSaving(null)
    }
  }

  const saveCoa = async () => {
    if (!coa) return
    try {
      setSaving('coa')
      const payload = await coaForm.validateFields() as CoalescerConfig
      await putCoalescerConfig(payload)
      setCoa(payload)
      message.success(`${t('common:writeCoalescer')} ${t('common:success')}`)
    } catch (e) {
      console.error(e)
      message.error('Failed to update Write Coalescer config')
    } finally {
      setSaving(null)
    }
  }

  return (
    <div style={{ padding: 24 }}>
      <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            <ApiOutlined style={{ marginRight: 8 }} />
            {t('nav:items.runtimeConfig')}
            <Text type="secondary" style={{ fontSize: 14, fontWeight: 'normal', marginLeft: 12 }}>
              <BulbFilled style={{ color: '#faad14', marginRight: 4 }} />
              Changes are applied in-memory on the monitor node and remain effective until the next restart.
            </Text>
          </Title>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={() => void loadAll()} loading={loading}>
            {t('common:refresh')}
          </Button>
        </Col>
      </Row>

      <Card style={{ borderRadius: 12 }}>
        <Tabs
          defaultActiveKey="cb"
          items={[
            {
              key: 'cb',
              label: (
                <Space>
                  <ThunderboltOutlined />
                  {t('common:circuitBreaker')}
                </Space>
              ),
              children: (
                <Row gutter={[24, 16]}>
                  <Col xs={24} md={14}>
                    <Form
                      form={cbForm}
                      layout="vertical"
                      initialValues={cb ?? { failure_threshold: 50, recovery_timeout_ms: 5000, half_open_max_requests: 10 }}
                      disabled={loading}
                    >
                      <Form.Item
                        label={t('common:failureThreshold')}
                        name="failure_threshold"
                        rules={[{ required: true, min: 1, type: 'integer', message: 'Must be a positive integer' }]}
                      >
                        <InputNumber min={1} style={{ width: '100%' }} />
                      </Form.Item>
                      <Form.Item
                        label={t('common:recoveryTimeoutMs')}
                        name="recovery_timeout_ms"
                        rules={[{ required: true, min: 1, type: 'integer' }]}
                      >
                        <InputNumber min={1} style={{ width: '100%' }} addonAfter="ms" />
                      </Form.Item>
                      <Form.Item
                        label={t('common:halfOpenMaxRequests')}
                        name="half_open_max_requests"
                        rules={[{ required: true, min: 1, type: 'integer' }]}
                      >
                        <InputNumber min={1} style={{ width: '100%' }} />
                      </Form.Item>
                      <Space>
                        <Button
                          type="primary"
                          icon={<SaveOutlined />}
                          onClick={saveCb}
                          loading={saving === 'cb'}
                        >
                          {t('common:save')}
                        </Button>
                        <Button onClick={() => cbForm.setFieldsValue(cb)} disabled={!cb}>
                          Reset
                        </Button>
                      </Space>
                    </Form>
                  </Col>
                  <Col xs={24} md={10}>
                    <Card size="small" title="说明 / Notes" type="inner">
                      <ul style={{ margin: 0, paddingLeft: 20 }}>
                        <li>FUSE 客户端在请求连续失败超过阈值后将打开熔断器，暂时停止请求。</li>
                        <li>恢复超时后进入半开状态，允许少量请求探测后端健康度。</li>
                        <li>半开请求全部成功则关闭熔断器，恢复正常流量。</li>
                      </ul>
                    </Card>
                  </Col>
                </Row>
              ),
            },
            {
              key: 'coa',
              label: (
                <Space>
                  <ApiOutlined />
                  {t('common:writeCoalescer')}
                </Space>
              ),
              children: (
                <Row gutter={[24, 16]}>
                  <Col xs={24} md={14}>
                    <Form
                      form={coaForm}
                      layout="vertical"
                      initialValues={coa ?? { deadline_ms: 2000, min_pending_writes: 4, max_dirty_bytes_per_entry: 1048576, max_dirty_bytes_total: 67108864, disabled: false }}
                      disabled={loading}
                    >
                      <Form.Item
                        label={t('common:deadlineMs')}
                        name="deadline_ms"
                        rules={[{ required: true, min: 1, type: 'integer' }]}
                      >
                        <InputNumber min={1} style={{ width: '100%' }} addonAfter="ms" />
                      </Form.Item>
                      <Form.Item
                        label={t('common:minPendingWrites')}
                        name="min_pending_writes"
                        rules={[{ required: true, min: 1, type: 'integer' }]}
                      >
                        <InputNumber min={1} style={{ width: '100%' }} />
                      </Form.Item>
                      <Form.Item
                        label={t('common:maxDirtyBytesPerEntry')}
                        name="max_dirty_bytes_per_entry"
                        rules={[{ required: true, min: 4096, type: 'integer' }]}
                      >
                        <InputNumber min={4096} step={65536} style={{ width: '100%' }} addonAfter="B" />
                      </Form.Item>
                      <Form.Item
                        label={t('common:maxDirtyBytesTotal')}
                        name="max_dirty_bytes_total"
                        rules={[{ required: true, min: 65536, type: 'integer' }]}
                      >
                        <InputNumber min={65536} step={1048576} style={{ width: '100%' }} addonAfter="B" />
                      </Form.Item>
                      <Form.Item label={t('common:disabled')} name="disabled" valuePropName="checked">
                        <Switch />
                      </Form.Item>
                      <Space>
                        <Button
                          type="primary"
                          icon={<SaveOutlined />}
                          onClick={saveCoa}
                          loading={saving === 'coa'}
                        >
                          {t('common:save')}
                        </Button>
                        <Button onClick={() => coaForm.setFieldsValue(coa)} disabled={!coa}>
                          Reset
                        </Button>
                      </Space>
                    </Form>
                  </Col>
                  <Col xs={24} md={10}>
                    <Card size="small" title="说明 / Notes" type="inner">
                      <ul style={{ margin: 0, paddingLeft: 20 }}>
                        <li>写合并器会合并短时间内针对同一 inode 的多个 write，减少实际落盘次数。</li>
                        <li>当任一条目脏字节数达到上限或到达死线时触发 flush。</li>
                        <li>临时调试或对比性能时可勾选 Disabled 来关闭合并。</li>
                      </ul>
                    </Card>
                  </Col>
                </Row>
              ),
            },
          ]}
        />
      </Card>
    </div>
  )
}

export default RuntimeConfig
