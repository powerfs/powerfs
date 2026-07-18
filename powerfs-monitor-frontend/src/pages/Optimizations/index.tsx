import React, { useState, useEffect } from 'react';
import {
  Card,
  Typography,
  Row,
  Col,
  Button,
  Switch,
  Table,
  Spin,
  Alert,
  Space,
  Tag,
} from 'antd';
import {
  ArrowUpOutlined,
  ArrowDownOutlined,
  PlayCircleOutlined,
  RotateLeftOutlined,
  DatabaseOutlined,
  DesktopOutlined,
  DashboardOutlined,
} from '@ant-design/icons';

interface OptimizationFlags {
  ec_simd_enabled: boolean;
  ec_parallel_encoding: boolean;
  ec_dynamic_sharding: boolean;
  ec_small_file_skip: boolean;
  raft_log_compression: boolean;
  raft_pre_vote: boolean;
  raft_read_scaling: boolean;
  rack_awareness: boolean;
  load_balancing: boolean;
  smart_cache_eviction: boolean;
  hierarchical_index: boolean;
}

interface BenchmarkMetrics {
  ec_throughput_mbps: number;
  ec_latency_ms: number;
  raft_election_time_ms: number;
  kv_cache_hit_rate: number;
  kv_read_throughput_ops: number;
  kv_write_throughput_ops: number;
  s3_read_throughput_mbps: number;
  s3_write_throughput_mbps: number;
  data_balance_score: number;
  cpu_usage_percent: number;
  memory_usage_percent: number;
}

interface EnvironmentInfo {
  cpu_model: string;
  cpu_cores: number;
  memory_gb: number;
  node_count: number;
  os_version: string;
  rust_version: string;
  powerfs_version: string;
}

interface BenchmarkResult {
  id: string;
  test_name: string;
  timestamp: string;
  flags: OptimizationFlags;
  metrics: BenchmarkMetrics;
  environment: EnvironmentInfo;
  duration_seconds: number;
  comparison?: ComparisonReport;
}

interface ComparisonReport {
  baseline_id: string;
  target_id: string;
  ec_throughput_improvement: number;
  ec_latency_improvement: number;
  raft_election_improvement: number;
  kv_cache_hit_rate_improvement: number;
  kv_read_throughput_improvement: number;
  kv_write_throughput_improvement: number;
  s3_read_throughput_improvement: number;
  s3_write_throughput_improvement: number;
  data_balance_improvement: number;
}

const flagNames: Record<string, string> = {
  ec_simd_enabled: 'EC SIMD 编码',
  ec_parallel_encoding: 'EC 并行编码',
  ec_dynamic_sharding: 'EC 动态分片',
  ec_small_file_skip: 'EC 小文件跳过',
  raft_log_compression: 'Raft 日志压缩',
  raft_pre_vote: 'Raft 预投票',
  raft_read_scaling: 'Raft 读扩展',
  rack_awareness: '机架感知',
  load_balancing: '负载均衡',
  smart_cache_eviction: '智能缓存淘汰',
  hierarchical_index: '分层索引',
};

const OptimizationDashboard: React.FC = () => {
  const [flags, setFlags] = useState<OptimizationFlags | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [results, setResults] = useState<BenchmarkResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  const fetchFlags = async () => {
    try {
      const response = await fetch('/api/optimizations');
      const data = await response.json();
      setFlags(data.flags);
    } catch (err) {
      setError('Failed to fetch optimization flags');
    }
  };

  const fetchResults = async () => {
    try {
      const response = await fetch('/api/benchmark/results?limit=20');
      const data = await response.json();
      setResults(data);
    } catch (err) {
      setError('Failed to fetch benchmark results');
    }
  };

  useEffect(() => {
    fetchFlags();
    fetchResults();
  }, []);

  const handleFlagChange = async (flagName: string, value: boolean) => {
    if (!flags) return;

    try {
      const response = await fetch(`/api/optimizations/${flagName}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value }),
      });

      if (response.ok) {
        setFlags((prev) => prev ? { ...prev, [flagName]: value } : null);
      } else {
        setError('Failed to update optimization flag');
      }
    } catch (err) {
      setError('Failed to update optimization flag');
    }
  };

  const handleReset = async () => {
    try {
      await fetch('/api/optimizations/reset', { method: 'POST' });
      await fetchFlags();
    } catch (err) {
      setError('Failed to reset flags');
    }
  };

  const handleBaseline = async () => {
    try {
      await fetch('/api/optimizations/baseline', { method: 'POST' });
      await fetchFlags();
    } catch (err) {
      setError('Failed to set baseline');
    }
  };

  const handleRunBenchmark = async () => {
    setIsRunning(true);
    try {
      const response = await fetch('/api/benchmark/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ test_duration_seconds: 30 }),
      });

      if (response.ok) {
        await fetchResults();
      } else {
        const data = await response.json();
        setError(data.message || 'Failed to run benchmark');
      }
    } catch (err) {
      setError('Failed to run benchmark');
    } finally {
      setIsRunning(false);
    }
  };

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

  const MetricCard: React.FC<{
    title: string;
    value: string | number;
    unit?: string;
    improvement?: number;
    icon: React.ReactNode;
    color: 'primary' | 'secondary' | 'success' | 'warning';
  }> = ({ title, value, unit, improvement, icon, color }) => {
    const colorMap = {
      primary: { border: '2px solid #1890ff', iconColor: '#1890ff' },
      secondary: { border: '2px solid #722ED1', iconColor: '#722ED1' },
      success: { border: '2px solid #52c41a', iconColor: '#52c41a' },
      warning: { border: '2px solid #fa8c16', iconColor: '#fa8c16' },
    };

    return (
      <Card style={{ borderLeft: colorMap[color].border }}>
        <Space direction="horizontal" size="middle" align="center">
          <div style={{ fontSize: 28, color: colorMap[color].iconColor }}>
            {icon}
          </div>
          <div>
            <Typography.Text type="secondary">{title}</Typography.Text>
            <div style={{ fontSize: 24, fontWeight: 'bold', marginTop: 4 }}>
              {value}
              {unit && <span style={{ fontSize: 14, fontWeight: 'normal', color: '#999', marginLeft: 4 }}>{unit}</span>}
            </div>
            {improvement !== undefined && (
              <div style={{ marginTop: 4 }}>
                {improvement >= 0 ? (
                  <Tag color="green" icon={<ArrowUpOutlined />}>
                    +{improvement.toFixed(1)}%
                  </Tag>
                ) : (
                  <Tag color="red" icon={<ArrowDownOutlined />}>
                    {improvement.toFixed(1)}%
                  </Tag>
                )}
              </div>
            )}
          </div>
        </Space>
      </Card>
    );
  };

  const latestResult = results[0];

  const flagColumns = [
    {
      title: '优化项',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '状态',
      dataIndex: 'value',
      key: 'value',
      render: (value: boolean) => (
        <Tag color={value ? 'green' : 'default'}>
          {value ? '启用' : '禁用'}
        </Tag>
      ),
    },
    {
      title: '操作',
      dataIndex: 'key',
      key: 'action',
      render: (_: string, record: { key: string; value: boolean }) => (
        <Switch
          checked={record.value}
          onChange={(checked) => handleFlagChange(record.key, checked)}
        />
      ),
    },
  ];

  const flagDataSource = flags ? Object.entries(flags).map(([key, value]) => ({
    key,
    name: flagNames[key] || key,
    value,
  })) : [];

  const resultColumns = [
    {
      title: '测试 ID',
      dataIndex: 'id',
      key: 'id',
      ellipsis: true,
    },
    {
      title: '时间',
      dataIndex: 'timestamp',
      key: 'timestamp',
      render: (ts: string) => formatTime(ts),
    },
    {
      title: 'EC 吞吐量 (MB/s)',
      dataIndex: ['metrics', 'ec_throughput_mbps'],
      key: 'ec_throughput',
      render: (v: number) => v.toFixed(1),
    },
    {
      title: 'EC 延迟 (ms)',
      dataIndex: ['metrics', 'ec_latency_ms'],
      key: 'ec_latency',
      render: (v: number) => v.toFixed(2),
    },
    {
      title: 'Raft 选举 (ms)',
      dataIndex: ['metrics', 'raft_election_time_ms'],
      key: 'raft_election',
      render: (v: number) => v.toFixed(0),
    },
    {
      title: 'KV 命中率',
      dataIndex: ['metrics', 'kv_cache_hit_rate'],
      key: 'kv_hit_rate',
      render: (v: number) => `${(v * 100).toFixed(1)}%`,
    },
    {
      title: '持续时间',
      dataIndex: 'duration_seconds',
      key: 'duration',
      render: (v: number) => `${v} 秒`,
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Typography.Title level={4}>优化效果监控面板</Typography.Title>

      {error && (
        <Alert
          message="错误"
          description={error}
          type="error"
          style={{ marginBottom: 16 }}
        />
      )}

      <Space style={{ marginBottom: 24 }}>
        <Button
          type="primary"
          onClick={handleRunBenchmark}
          disabled={isRunning}
          icon={isRunning ? <Spin size="small" /> : <PlayCircleOutlined />}
        >
          {isRunning ? '运行中...' : '运行基准测试'}
        </Button>
        <Button onClick={handleReset} icon={<RotateLeftOutlined />}>
          重置为默认值
        </Button>
        <Button onClick={handleBaseline}>设置为基线（全关）</Button>
      </Space>

      <Card title="优化开关状态" style={{ marginBottom: 24 }}>
        {flags ? (
          <Table
            dataSource={flagDataSource}
            columns={flagColumns}
            pagination={false}
            rowKey="key"
          />
        ) : (
          <Spin />
        )}
      </Card>

      {latestResult && (
        <div style={{ marginBottom: 24 }}>
          <Typography.Title level={5} style={{ marginBottom: 16 }}>
            最新测试结果 - {formatTime(latestResult.timestamp)}
          </Typography.Title>
          <Row gutter={[16, 16]}>
            <Col xs={24} sm={12} md={6}>
              <MetricCard
                title="EC 吞吐量"
                value={latestResult.metrics.ec_throughput_mbps.toFixed(1)}
                unit="MB/s"
                improvement={latestResult.comparison?.ec_throughput_improvement}
                icon={<DashboardOutlined />}
                color="primary"
              />
            </Col>
            <Col xs={24} sm={12} md={6}>
              <MetricCard
                title="EC 延迟"
                value={latestResult.metrics.ec_latency_ms.toFixed(2)}
                unit="ms"
                improvement={latestResult.comparison?.ec_latency_improvement}
                icon={<DashboardOutlined />}
                color="secondary"
              />
            </Col>
            <Col xs={24} sm={12} md={6}>
              <MetricCard
                title="Raft 选举时间"
                value={latestResult.metrics.raft_election_time_ms.toFixed(0)}
                unit="ms"
                improvement={latestResult.comparison?.raft_election_improvement}
                icon={<DatabaseOutlined />}
                color="success"
              />
            </Col>
            <Col xs={24} sm={12} md={6}>
              <MetricCard
                title="KV 缓存命中率"
                value={(latestResult.metrics.kv_cache_hit_rate * 100).toFixed(1)}
                unit="%"
                improvement={latestResult.comparison?.kv_cache_hit_rate_improvement}
                icon={<DatabaseOutlined />}
                color="warning"
              />
            </Col>
            <Col xs={24} sm={12} md={6}>
              <MetricCard
                title="KV 读吞吐"
                value={latestResult.metrics.kv_read_throughput_ops.toFixed(0)}
                unit="ops/s"
                improvement={latestResult.comparison?.kv_read_throughput_improvement}
                icon={<DesktopOutlined />}
                color="primary"
              />
            </Col>
            <Col xs={24} sm={12} md={6}>
              <MetricCard
                title="KV 写吞吐"
                value={latestResult.metrics.kv_write_throughput_ops.toFixed(0)}
                unit="ops/s"
                improvement={latestResult.comparison?.kv_write_throughput_improvement}
                icon={<DesktopOutlined />}
                color="secondary"
              />
            </Col>
            <Col xs={24} sm={12} md={6}>
              <MetricCard
                title="S3 读吞吐"
                value={latestResult.metrics.s3_read_throughput_mbps.toFixed(1)}
                unit="MB/s"
                improvement={latestResult.comparison?.s3_read_throughput_improvement}
                icon={<DatabaseOutlined />}
                color="success"
              />
            </Col>
            <Col xs={24} sm={12} md={6}>
              <MetricCard
                title="数据均衡度"
                value={(latestResult.metrics.data_balance_score * 100).toFixed(1)}
                unit="%"
                improvement={latestResult.comparison?.data_balance_improvement}
                icon={<DashboardOutlined />}
                color="warning"
              />
            </Col>
          </Row>
        </div>
      )}

      {latestResult && (
        <Card title="测试环境信息" style={{ marginBottom: 24 }}>
          <Row gutter={[16, 16]}>
            <Col xs={24} sm={12} md={6}>
              <Typography.Text type="secondary">CPU 型号</Typography.Text>
              <div style={{ fontWeight: 'bold' }}>{latestResult.environment.cpu_model}</div>
            </Col>
            <Col xs={24} sm={12} md={6}>
              <Typography.Text type="secondary">CPU 核心数</Typography.Text>
              <div style={{ fontWeight: 'bold' }}>{latestResult.environment.cpu_cores} 核</div>
            </Col>
            <Col xs={24} sm={12} md={6}>
              <Typography.Text type="secondary">内存</Typography.Text>
              <div style={{ fontWeight: 'bold' }}>{latestResult.environment.memory_gb.toFixed(1)} GB</div>
            </Col>
            <Col xs={24} sm={12} md={6}>
              <Typography.Text type="secondary">节点数</Typography.Text>
              <div style={{ fontWeight: 'bold' }}>{latestResult.environment.node_count} 节点</div>
            </Col>
            <Col xs={24} sm={12} md={6}>
              <Typography.Text type="secondary">操作系统</Typography.Text>
              <div style={{ fontWeight: 'bold' }}>{latestResult.environment.os_version}</div>
            </Col>
            <Col xs={24} sm={12} md={6}>
              <Typography.Text type="secondary">Rust 版本</Typography.Text>
              <div style={{ fontWeight: 'bold' }}>{latestResult.environment.rust_version}</div>
            </Col>
            <Col xs={24} sm={12} md={6}>
              <Typography.Text type="secondary">PowerFS 版本</Typography.Text>
              <div style={{ fontWeight: 'bold' }}>{latestResult.environment.powerfs_version}</div>
            </Col>
          </Row>
        </Card>
      )}

      <Card title="历史测试结果">
        <Table
          dataSource={results}
          columns={resultColumns}
          rowKey="id"
          pagination={{ pageSize: 10 }}
        />
      </Card>
    </div>
  );
};

export default OptimizationDashboard;