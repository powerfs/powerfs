import React, { useState, useEffect } from 'react';
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
} from 'antd';
import {
  DesktopOutlined,
  DashboardOutlined,
  InfoCircleOutlined,
  SafetyOutlined,
  ThunderboltOutlined,
  CloudServerOutlined,
} from '@ant-design/icons';
import { getBenchmarkResults } from '@/services/api';

// Real runtime config values from the codebase (read-only display).
// These will become dynamic via backend APIs in Phase B.
const circuitBreakerConfig = {
  failure_threshold: 50,
  recovery_timeout_ms: 5000,
  half_open_max_requests: 10,
};

const coalescerConfig = {
  deadline_ms: 2000,
  min_pending_writes: 4,
  max_dirty_bytes_per_entry: 1048576, // 1 MB
  max_dirty_bytes_total: 67108864,    // 64 MB
  disabled: false,
};

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

  useEffect(() => {
    const fetchResults = async () => {
      try {
        const data = await getBenchmarkResults();
        setResults(data as unknown as BenchmarkResult[]);
      } catch (err) {
        setError('加载基准测试结果失败');
      } finally {
        setLoading(false);
      }
    };
    fetchResults();
  }, []);

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
            运行时配置展示当前系统中各客户端优化组件的实际参数。这些参数在代码中定义，
            Phase B 将通过后端 API 支持动态查看和修改。
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
            title={<Space><SafetyOutlined /> 熔断器配置</Space>}
            size="small"
          >
            <Descriptions column={1} size="small">
              <Descriptions.Item label="失败阈值">
                {circuitBreakerConfig.failure_threshold} 次连续失败后熔断
              </Descriptions.Item>
              <Descriptions.Item label="恢复超时">
                {circuitBreakerConfig.recovery_timeout_ms / 1000} 秒后进入半开状态
              </Descriptions.Item>
              <Descriptions.Item label="半开探测请求数">
                {circuitBreakerConfig.half_open_max_requests} 个探测请求成功后恢复
              </Descriptions.Item>
            </Descriptions>
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card
            title={<Space><ThunderboltOutlined /> 写合并配置</Space>}
            size="small"
          >
            <Descriptions column={1} size="small">
              <Descriptions.Item label="刷新截止时间">
                {coalescerConfig.deadline_ms / 1000} 秒
              </Descriptions.Item>
              <Descriptions.Item label="最小待处理写入次数">
                {coalescerConfig.min_pending_writes} 次后触发刷新
              </Descriptions.Item>
              <Descriptions.Item label="单条最大脏字节数">
                {formatBytes(coalescerConfig.max_dirty_bytes_per_entry)}
              </Descriptions.Item>
              <Descriptions.Item label="总最大脏字节数">
                {formatBytes(coalescerConfig.max_dirty_bytes_total)}
              </Descriptions.Item>
              <Descriptions.Item label="合并模式">
                <Tag color={coalescerConfig.disabled ? 'red' : 'green'}>
                  {coalescerConfig.disabled ? '已禁用' : '已启用'}
                </Tag>
              </Descriptions.Item>
            </Descriptions>
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
    </div>
  );
};

export default OptimizationDashboard;
