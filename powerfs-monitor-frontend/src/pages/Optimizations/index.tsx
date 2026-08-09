import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Typography,
  Row,
  Col,
  Table,
  Spin,
  Alert,
  Space,
  Tag,
  Descriptions,
  Badge,
  Button,
  Form,
  InputNumber,
  Switch,
  Modal,
  message,
  Tooltip,
} from 'antd';
import {
  DesktopOutlined,
  DashboardOutlined,
  InfoCircleOutlined,
  SafetyOutlined,
  ThunderboltOutlined,
  CloudServerOutlined,
  EditOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import {
  getBenchmarkResults,
  getCircuitBreakerConfig,
  putCircuitBreakerConfig,
  getCoalescerConfig,
  putCoalescerConfig,
} from '@/services/api';
import type { CircuitBreakerConfig, CoalescerConfig } from '@/types';

// Scheduler priorities & connection pool config are static design constants
// (not runtime-mutable yet); keep them as read-only reference tables.
const schedulerPriorities = [
  { kind: 'Read (读)', priority: 1, description: '最高优先级，确保读请求不被写洪峰阻塞' },
  { kind: 'Lease (续租)', priority: 2, description: '高优先级，防止 Lease 过期导致客户端失活' },
  { kind: 'Write (写)', priority: 3, description: '中优先级，合并写入后批量处理' },
  { kind: 'Management (管理)', priority: 4, description: '低优先级，后台管理操作' },
];

const connectionPoolConfig = {
  keepalive_idle_secs: 60,
  keepalive_interval_secs: 10,
  keepalive_probes: 3,
  health_check_interval_secs: 15,
};

interface BenchmarkResult {
  id: string;
  type: string;
  status: string;
  started_at: string;
  completed_at?: string;
  result?: {
    benchmark: string;
    timestamp: string;
    summary: Record<string, {
      avg_ops_per_sec?: number;
      avg_latency_ms?: number;
      avg_bandwidth_mbps?: number;
    }>;
  };
}

const formatBytes = (bytes: number): string => {
  if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(1)} GB`;
  if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
};

const OptimizationDashboard: React.FC = () => {
  const [results, setResults] = useState<BenchmarkResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Runtime config (hot-modify via PUT /api/config/*)
  const [cbConfig, setCbConfig] = useState<CircuitBreakerConfig | null>(null);
  const [coConfig, setCoConfig] = useState<CoalescerConfig | null>(null);
  const [cbLoading, setCbLoading] = useState(false);
  const [coLoading, setCoLoading] = useState(false);
  const [cbEditing, setCbEditing] = useState(false);
  const [coEditing, setCoEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [cbForm] = Form.useForm<CircuitBreakerConfig>();
  const [coForm] = Form.useForm<CoalescerConfig>();

  const loadBenchmark = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getBenchmarkResults();
      setResults(data as unknown as BenchmarkResult[]);
      setError(null);
    } catch (err) {
      setError('加载基准测试结果失败');
    } finally {
      setLoading(false);
    }
  }, []);

  const loadCbConfig = useCallback(async () => {
    setCbLoading(true);
    try {
      const cfg = await getCircuitBreakerConfig();
      setCbConfig(cfg);
    } catch (err) {
      // 后端未实现时静默, 保持 null 显示占位
      console.warn('Failed to load circuit breaker config:', err);
    } finally {
      setCbLoading(false);
    }
  }, []);

  const loadCoConfig = useCallback(async () => {
    setCoLoading(true);
    try {
      const cfg = await getCoalescerConfig();
      setCoConfig(cfg);
    } catch (err) {
      console.warn('Failed to load coalescer config:', err);
    } finally {
      setCoLoading(false);
    }
  }, []);

  useEffect(() => {
    loadBenchmark();
    loadCbConfig();
    loadCoConfig();
  }, [loadBenchmark, loadCbConfig, loadCoConfig]);

  const formatTime = (timestamp: string) => {
    return new Date(timestamp).toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  // ── 编辑熔断器配置 ──
  const handleEditCb = () => {
    if (!cbConfig) return;
    cbForm.setFieldsValue(cbConfig);
    setCbEditing(true);
  };

  const handleSaveCb = async () => {
    try {
      const values = await cbForm.validateFields();
      setSaving(true);
      await putCircuitBreakerConfig(values);
      setCbConfig(values);
      setCbEditing(false);
      message.success('熔断器配置已更新（in-memory，重启后失效）');
    } catch (err: any) {
      if (err?.errorFields) return; // 表单校验错误, 不关闭 Modal
      message.error('更新熔断器配置失败');
    } finally {
      setSaving(false);
    }
  };

  // ── 编辑写合并配置 ──
  const handleEditCo = () => {
    if (!coConfig) return;
    coForm.setFieldsValue(coConfig);
    setCoEditing(true);
  };

  const handleSaveCo = async () => {
    try {
      const values = await coForm.validateFields();
      setSaving(true);
      await putCoalescerConfig(values);
      setCoConfig(values);
      setCoEditing(false);
      message.success('写合并配置已更新（in-memory，重启后失效）');
    } catch (err: any) {
      if (err?.errorFields) return;
      message.error('更新写合并配置失败');
    } finally {
      setSaving(false);
    }
  };

  const benchmarkColumns = [
    {
      title: '类型',
      dataIndex: 'type',
      key: 'type',
      width: 100,
      render: (type: string) => {
        const colors: Record<string, string> = { kv: 'blue', metadata: 'purple', fs: 'green', s3: 'orange' };
        return <Tag color={colors[type] || 'default'}>{type.toUpperCase()}</Tag>;
      },
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 80,
      render: (status: string) => (
        <Badge status={status === 'completed' ? 'success' : status === 'running' ? 'processing' : 'error'} text={status} />
      ),
    },
    {
      title: '时间',
      dataIndex: 'started_at',
      key: 'started_at',
      render: (ts: string) => formatTime(ts),
    },
    {
      title: '关键指标',
      key: 'summary',
      render: (_: unknown, record: BenchmarkResult) => {
        if (!record.result?.summary) return '-';
        const entries = Object.entries(record.result.summary).slice(0, 3);
        return (
          <Space size={4} wrap>
            {entries.map(([op, metrics]) => (
              <Tag key={op} style={{ fontSize: 11 }}>
                {op}: {metrics.avg_ops_per_sec ? `${(metrics.avg_ops_per_sec / 1000).toFixed(1)}K ops/s` : ''}
                {metrics.avg_bandwidth_mbps ? `${metrics.avg_bandwidth_mbps.toFixed(0)} MB/s` : ''}
                {metrics.avg_latency_ms ? ` ${metrics.avg_latency_ms.toFixed(3)}ms` : ''}
              </Tag>
            ))}
          </Space>
        );
      },
    },
  ];

  return (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            运行时配置通过 <Typography.Text code style={{ fontSize: 12 }}>GET/PUT /api/config/*</Typography.Text> 实时读取和修改。
            修改保存在 Monitor 内存中（重启后失效）。所有写操作要求 admin 权限。
          </Typography.Text>
        </div>
      </Card>

      {error && (
        <Alert
          message="错误"
          description={error}
          type="error"
          style={{ marginBottom: 16 }}
        />
      )}

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} lg={12}>
          <Card
            title={
              <Space>
                <SafetyOutlined /> 熔断器配置
                {cbConfig && <Tag color="green" style={{ fontSize: 11}}>实时</Tag>}
              </Space>
            }
            size="small"
            extra={
              <Space>
                <Tooltip title="刷新">
                  <Button icon={<ReloadOutlined />} onClick={loadCbConfig} size="small" loading={cbLoading} />
                </Tooltip>
                <Button
                  icon={<EditOutlined />}
                  size="small"
                  onClick={handleEditCb}
                  disabled={!cbConfig}
                >
                  编辑
                </Button>
              </Space>
            }
          >
            <Spin spinning={cbLoading && !cbConfig}>
              {cbConfig ? (
                <Descriptions column={1} size="small">
                  <Descriptions.Item label="失败阈值">
                    {cbConfig.failure_threshold} 次连续失败后熔断
                  </Descriptions.Item>
                  <Descriptions.Item label="恢复超时">
                    {cbConfig.recovery_timeout_ms / 1000} 秒后进入半开状态
                  </Descriptions.Item>
                  <Descriptions.Item label="半开探测请求数">
                    {cbConfig.half_open_max_requests} 个探测请求成功后恢复
                  </Descriptions.Item>
                </Descriptions>
              ) : (
                <Typography.Text type="secondary">
                  {cbLoading ? '加载中...' : '无法加载配置 (后端未实现或要求 admin 权限)'}
                </Typography.Text>
              )}
            </Spin>
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card
            title={
              <Space>
                <ThunderboltOutlined /> 写合并配置
                {coConfig && <Tag color="green" style={{ fontSize: 11}}>实时</Tag>}
              </Space>
            }
            size="small"
            extra={
              <Space>
                <Tooltip title="刷新">
                  <Button icon={<ReloadOutlined />} onClick={loadCoConfig} size="small" loading={coLoading} />
                </Tooltip>
                <Button
                  icon={<EditOutlined />}
                  size="small"
                  onClick={handleEditCo}
                  disabled={!coConfig}
                >
                  编辑
                </Button>
              </Space>
            }
          >
            <Spin spinning={coLoading && !coConfig}>
              {coConfig ? (
                <Descriptions column={1} size="small">
                  <Descriptions.Item label="刷新截止时间">
                    {coConfig.deadline_ms / 1000} 秒
                  </Descriptions.Item>
                  <Descriptions.Item label="最小待处理写入次数">
                    {coConfig.min_pending_writes} 次后触发刷新
                  </Descriptions.Item>
                  <Descriptions.Item label="单条最大脏字节数">
                    {formatBytes(coConfig.max_dirty_bytes_per_entry)}
                  </Descriptions.Item>
                  <Descriptions.Item label="总最大脏字节数">
                    {formatBytes(coConfig.max_dirty_bytes_total)}
                  </Descriptions.Item>
                  <Descriptions.Item label="合并模式">
                    <Tag color={coConfig.disabled ? 'red' : 'green'}>
                      {coConfig.disabled ? '已禁用' : '已启用'}
                    </Tag>
                  </Descriptions.Item>
                </Descriptions>
              ) : (
                <Typography.Text type="secondary">
                  {coLoading ? '加载中...' : '无法加载配置 (后端未实现或要求 admin 权限)'}
                </Typography.Text>
              )}
            </Spin>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} lg={12}>
          <Card
            title={<Space><DashboardOutlined /> 多队列调度优先级</Space>}
            size="small"
          >
            <Table
              dataSource={schedulerPriorities}
              rowKey="kind"
              size="small"
              pagination={false}
              columns={[
                { title: '请求类型', dataIndex: 'kind', key: 'kind', width: 120 },
                { title: '优先级', dataIndex: 'priority', key: 'priority', width: 70,
                  render: (p: number) => <Tag color={p === 1 ? 'red' : p === 2 ? 'orange' : 'blue'}>{p}</Tag> },
                { title: '说明', dataIndex: 'description', key: 'description' },
              ]}
            />
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card
            title={<Space><CloudServerOutlined /> 连接池健康配置</Space>}
            size="small"
          >
            <Descriptions column={1} size="small">
              <Descriptions.Item label="TCP Keepalive 空闲时间">
                {connectionPoolConfig.keepalive_idle_secs} 秒
              </Descriptions.Item>
              <Descriptions.Item label="TCP Keepalive 探测间隔">
                {connectionPoolConfig.keepalive_interval_secs} 秒
              </Descriptions.Item>
              <Descriptions.Item label="TCP Keepalive 探测次数">
                {connectionPoolConfig.keepalive_probes} 次
              </Descriptions.Item>
              <Descriptions.Item label="健康巡检间隔">
                {connectionPoolConfig.health_check_interval_secs} 秒
              </Descriptions.Item>
            </Descriptions>
          </Card>
        </Col>
      </Row>

      <Card title={<Space><DesktopOutlined /> 基准测试结果</Space>} style={{ marginBottom: 16 }}>
        {loading ? (
          <div style={{ textAlign: 'center', padding: 40 }}><Spin /></div>
        ) : (
          <Table
            dataSource={results}
            columns={benchmarkColumns}
            rowKey="id"
            size="small"
            pagination={{ pageSize: 10 }}
            locale={{ emptyText: '暂无测试记录' }}
          />
        )}
      </Card>

      {/* 编辑熔断器配置 Modal */}
      <Modal
        title="编辑熔断器配置"
        open={cbEditing}
        onCancel={() => setCbEditing(false)}
        onOk={handleSaveCb}
        confirmLoading={saving}
        okText="保存"
        cancelText="取消"
        destroyOnClose
      >
        <Form form={cbForm} layout="vertical" preserve={false}>
          <Form.Item
            name="failure_threshold"
            label="失败阈值 (次)"
            tooltip="连续失败次数达到此值后触发熔断"
            rules={[{ required: true, message: '请输入失败阈值' }, { type: 'number', min: 1, message: '必须 ≥ 1' }]}
          >
            <InputNumber min={1} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="recovery_timeout_ms"
            label="恢复超时 (毫秒)"
            tooltip="熔断后等待多久进入半开状态"
            rules={[{ required: true, message: '请输入恢复超时' }, { type: 'number', min: 100, message: '必须 ≥ 100ms' }]}
          >
            <InputNumber min={100} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="half_open_max_requests"
            label="半开探测请求数"
            tooltip="半开状态下允许的探测请求数, 全部成功后恢复"
            rules={[{ required: true, message: '请输入半开探测请求数' }, { type: 'number', min: 1, message: '必须 ≥ 1' }]}
          >
            <InputNumber min={1} style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑写合并配置 Modal */}
      <Modal
        title="编辑写合并配置"
        open={coEditing}
        onCancel={() => setCoEditing(false)}
        onOk={handleSaveCo}
        confirmLoading={saving}
        okText="保存"
        cancelText="取消"
        destroyOnClose
        width={520}
      >
        <Form form={coForm} layout="vertical" preserve={false}>
          <Form.Item
            name="deadline_ms"
            label="刷新截止时间 (毫秒)"
            tooltip="脏数据驻留最长时间, 超过此值强制刷新"
            rules={[{ required: true, message: '请输入刷新截止时间' }, { type: 'number', min: 100, message: '必须 ≥ 100ms' }]}
          >
            <InputNumber min={100} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="min_pending_writes"
            label="最小待处理写入次数"
            tooltip="待处理写入数达到此值触发刷新"
            rules={[{ required: true, message: '请输入最小待处理写入次数' }, { type: 'number', min: 1, message: '必须 ≥ 1' }]}
          >
            <InputNumber min={1} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="max_dirty_bytes_per_entry"
            label="单条最大脏字节数"
            tooltip="单个 entry 脏字节上限, 超过立即刷新"
            rules={[{ required: true, message: '请输入单条最大脏字节数' }, { type: 'number', min: 1024, message: '必须 ≥ 1KB' }]}
          >
            <InputNumber min={1024} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="max_dirty_bytes_total"
            label="总最大脏字节数"
            tooltip="全局脏字节上限, 超过立即刷新"
            rules={[{ required: true, message: '请输入总最大脏字节数' }, { type: 'number', min: 1024, message: '必须 ≥ 1KB' }]}
          >
            <InputNumber min={1024} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="disabled" label="禁用写合并" valuePropName="checked">
            <Switch checkedChildren="禁用" unCheckedChildren="启用" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default OptimizationDashboard;
