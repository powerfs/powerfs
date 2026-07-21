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
  Descriptions,
} from 'antd';
import {
  ArrowUpOutlined,
  ArrowDownOutlined,
  PlayCircleOutlined,
  RotateLeftOutlined,
  DatabaseOutlined,
  DesktopOutlined,
  DashboardOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons';
import {
  getOptimizationFlags,
  updateOptimizationFlag,
  resetOptimizationFlags,
  setOptimizationBaseline,
  runOptimizationBenchmark,
  getBenchmarkResults,
  type OptimizationFlags,
} from '@/services/api';

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

const flagDescriptions: Record<string, string> = {
  ec_simd_enabled: '使用 CPU SIMD 指令加速纠删码编码计算，可显著提升编码性能',
  ec_parallel_encoding: '启用并行编码处理，充分利用多核 CPU 资源',
  ec_dynamic_sharding: '根据数据大小动态选择分片策略，优化小文件处理效率',
  ec_small_file_skip: '小文件跳过纠删码编码，直接存储原始数据',
  raft_log_compression: '压缩 Raft 日志，减少网络传输和存储空间占用',
  raft_pre_vote: '启用预投票机制，减少不必要的选举开销',
  raft_read_scaling: '启用读扩展，允许 Follower 节点处理读请求',
  rack_awareness: '机架感知部署，确保副本分布在不同机架',
  load_balancing: '自动负载均衡，均衡各节点的存储和计算负载',
  smart_cache_eviction: '智能缓存淘汰策略，基于访问模式优化缓存命中率',
  hierarchical_index: '分层索引结构，加速元数据查询',
};

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

interface OptimizationBenchmarkResult {
  id: string;
  test_name: string;
  timestamp: string;
  flags: OptimizationFlags;
  metrics: BenchmarkMetrics;
  environment: EnvironmentInfo;
  duration_seconds: number;
}

const OptimizationDashboard: React.FC = () => {
  const [flags, setFlags] = useState<OptimizationFlags | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [results, setResults] = useState<OptimizationBenchmarkResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  const fetchFlags = async () => {
    try {
      const data = await getOptimizationFlags();
      setFlags(data.flags);
    } catch (err) {
      setError('加载优化配置失败');
    }
  };

  const fetchResults = async () => {
    try {
      const data = await getBenchmarkResults();
      setResults(data as unknown as OptimizationBenchmarkResult[]);
    } catch (err) {
      setError('加载基准测试结果失败');
    }
  };

  useEffect(() => {
    fetchFlags();
    fetchResults();
  }, []);

  const handleFlagChange = async (flagName: string, value: boolean) => {
    if (!flags) return;

    try {
      await updateOptimizationFlag(flagName, value);
      setFlags((prev) => prev ? { ...prev, [flagName]: value } : null);
    } catch (err) {
      setError('更新优化配置失败');
    }
  };

  const handleReset = async () => {
    try {
      await resetOptimizationFlags();
      await fetchFlags();
    } catch (err) {
      setError('重置配置失败');
    }
  };

  const handleBaseline = async () => {
    try {
      await setOptimizationBaseline();
      await fetchFlags();
    } catch (err) {
      setError('设置基线失败');
    }
  };

  const handleRunBenchmark = async () => {
    setIsRunning(true);
    try {
      await runOptimizationBenchmark();
      await fetchResults();
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : '运行基准测试失败';
      setError(errorMsg);
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
      width: 150,
    },
    {
      title: '说明',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
      render: (desc: string) => <Typography.Text type="secondary" style={{ fontSize: 12 }}>{desc}</Typography.Text>,
    },
    {
      title: '状态',
      dataIndex: 'value',
      key: 'value',
      width: 80,
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
      width: 80,
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
    description: flagDescriptions[key] || '',
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

  const enabledCount = flags ? Object.values(flags).filter(Boolean).length : 0;
  const totalCount = flags ? Object.keys(flags).length : 0;

  return (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <InfoCircleOutlined style={{ fontSize: 16, color: 'var(--pf-color-primary)' }} />
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            优化开关用于控制系统各组件的高级功能。这些功能默认已启用，可根据实际需求进行调整。
            修改后可运行基准测试评估性能变化。
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

      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col xs={24} md={6}>
          <Card>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 12, color: 'var(--pf-color-secondary)' }}>已启用优化</div>
              <div style={{ fontSize: 28, fontWeight: 700 }}>{enabledCount}/{totalCount}</div>
            </div>
          </Card>
        </Col>
        <Col xs={24} md={18}>
          <Space style={{ width: '100%', justifyContent: 'flex-end' }}>
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
        </Col>
      </Row>

      <Card title="优化开关状态" style={{ marginBottom: 24 }}>
        {flags ? (
          <Table
            dataSource={flagDataSource}
            columns={flagColumns}
            rowKey="key"
            size="small"
            pagination={false}
          />
        ) : (
          <Spin />
        )}
      </Card>

      {latestResult && (
        <Card title="最新基准测试结果" style={{ marginBottom: 24 }}>
          <Row gutter={[16, 16]}>
            <Col xs={24} sm={8}>
              <MetricCard
                title="EC 吞吐量"
                value={latestResult.metrics.ec_throughput_mbps.toFixed(1)}
                unit="MB/s"
                icon={<DesktopOutlined />}
                color="primary"
              />
            </Col>
            <Col xs={24} sm={8}>
              <MetricCard
                title="EC 延迟"
                value={latestResult.metrics.ec_latency_ms.toFixed(2)}
                unit="ms"
                icon={<DashboardOutlined />}
                color="secondary"
              />
            </Col>
            <Col xs={24} sm={8}>
              <MetricCard
                title="Raft 选举时间"
                value={latestResult.metrics.raft_election_time_ms.toFixed(0)}
                unit="ms"
                icon={<DatabaseOutlined />}
                color="success"
              />
            </Col>
            <Col xs={24} sm={8}>
              <MetricCard
                title="KV 缓存命中率"
                value={`${(latestResult.metrics.kv_cache_hit_rate * 100).toFixed(1)}%`}
                icon={<DatabaseOutlined />}
                color="warning"
              />
            </Col>
          </Row>
        </Card>
      )}

      <Card title="历史测试记录" style={{ marginBottom: 24 }}>
        <Table
          dataSource={results}
          columns={resultColumns}
          rowKey="id"
          size="small"
          pagination={{ pageSize: 10 }}
          locale={{ emptyText: '暂无测试记录' }}
        />
      </Card>

      <Card title="优化项说明" size="small">
        <Descriptions column={1} size="small">
          <Descriptions.Item label="EC SIMD 编码">
            使用 CPU 的 SIMD（单指令多数据）指令集加速纠删码编码计算，可显著提升大文件的编码性能。
            建议在支持 AVX2 或更高版本指令集的 CPU 上启用。
          </Descriptions.Item>
          <Descriptions.Item label="EC 并行编码">
            启用并行编码处理，将编码任务分配到多个 CPU 核心并行执行，充分利用多核 CPU 资源。
            在多核心服务器上可大幅提升编码吞吐量。
          </Descriptions.Item>
          <Descriptions.Item label="EC 动态分片">
            根据数据大小动态选择分片策略。对于小文件，使用更高效的分片大小，减少元数据开销；
            对于大文件，使用标准分片大小，优化存储效率。
          </Descriptions.Item>
          <Descriptions.Item label="Raft 日志压缩">
            压缩 Raft 日志条目，减少网络传输带宽和存储空间占用。适用于写入频繁的场景。
          </Descriptions.Item>
          <Descriptions.Item label="Raft 预投票">
            启用预投票机制，在正式选举前先进行预投票，减少不必要的选举开销，提升集群稳定性。
          </Descriptions.Item>
          <Descriptions.Item label="Raft 读扩展">
            允许 Follower 节点处理读请求，分散 Leader 节点的读负载，提升读吞吐量。
          </Descriptions.Item>
          <Descriptions.Item label="机架感知">
            确保副本分布在不同机架，提高数据的容错能力。需要在多机架部署环境中启用。
          </Descriptions.Item>
          <Descriptions.Item label="智能缓存淘汰">
            基于 LRU（最近最少使用）和访问频率的智能缓存淘汰策略，优化缓存命中率。
          </Descriptions.Item>
        </Descriptions>
      </Card>
    </div>
  );
};

export default OptimizationDashboard;