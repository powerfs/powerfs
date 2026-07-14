
import React, { useState, useEffect } from 'react';
import {
  Card,
  CardContent,
  Typography,
  Grid,
  Paper,
  Button,
  Switch,
  FormControlLabel,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  CircularProgress,
  Alert,
  AlertTitle,
  Box,
  Chip,
  Tooltip,
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  PlayCircle,
  RotateCcw,
  Database,
  Cpu,
  Memory,
  Activity,
} from '@mui/icons-material';

// API 类型定义
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

// 优化开关名称映射
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

// 主组件
const OptimizationDashboard: React.FC = () => {
  const [flags, setFlags] = useState<OptimizationFlags | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [results, setResults] = useState<BenchmarkResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  // 获取当前优化开关状态
  const fetchFlags = async () => {
    try {
      const response = await fetch('/api/optimizations');
      const data = await response.json();
      setFlags(data.flags);
    } catch (err) {
      setError('Failed to fetch optimization flags');
    }
  };

  // 获取历史测试结果
  const fetchResults = async () => {
    try {
      const response = await fetch('/api/benchmark/results?limit=20');
      const data = await response.json();
      setResults(data);
    } catch (err) {
      setError('Failed to fetch benchmark results');
    }
  };

  // 初始化加载
  useEffect(() => {
    fetchFlags();
    fetchResults();
  }, []);

  // 更新单个开关
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

  // 重置为默认值
  const handleReset = async () => {
    try {
      await fetch('/api/optimizations/reset', { method: 'POST' });
      await fetchFlags();
    } catch (err) {
      setError('Failed to reset flags');
    }
  };

  // 重置为基线
  const handleBaseline = async () => {
    try {
      await fetch('/api/optimizations/baseline', { method: 'POST' });
      await fetchFlags();
    } catch (err) {
      setError('Failed to set baseline');
    }
  };

  // 运行基准测试
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

  // 格式化时间
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

  // 渲染指标卡片
  const MetricCard: React.FC<{
    title: string;
    value: string | number;
    unit?: string;
    improvement?: number;
    icon: React.ReactNode;
    color: 'primary' | 'secondary' | 'success' | 'warning';
  }> = ({ title, value, unit, improvement, icon, color }) => {
    const colorMap = {
      primary: { bg: 'rgba(59, 130, 246, 0.1)', text: 'text-blue-600' },
      secondary: { bg: 'rgba(139, 92, 246, 0.1)', text: 'text-purple-600' },
      success: { bg: 'rgba(34, 197, 94, 0.1)', text: 'text-green-600' },
      warning: { bg: 'rgba(249, 115, 22, 0.1)', text: 'text-orange-600' },
    };

    return (
      <Card sx={{ bgcolor: colorMap[color].bg }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item>
              <Box sx={{ display: 'flex', alignItems: 'center', color: colorMap[color].text }}>
                {icon}
              </Box>
            </Grid>
            <Grid item xs>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                {title}
              </Typography>
              <Typography variant="h4" component="div" fontWeight="bold">
                {value}
                {unit && <span className="ml-1 text-sm font-normal text-gray-500">{unit}</span>}
              </Typography>
              {improvement !== undefined && (
                <Box sx={{ mt: 1 }}>
                  {improvement >= 0 ? (
                    <Chip
                      icon={<TrendingUp style={{ fontSize: 14 }} />}
                      label={`+${improvement.toFixed(1)}%`}
                      size="small"
                      color="success"
                      variant="outlined"
                    />
                  ) : (
                    <Chip
                      icon={<TrendingDown style={{ fontSize: 14 }} />}
                      label={`${improvement.toFixed(1)}%`}
                      size="small"
                      color="error"
                      variant="outlined"
                    />
                  )}
                </Box>
              )}
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    );
  };

  // 获取最新结果
  const latestResult = results[0];

  return (
    <div className="p-6">
      <Typography variant="h4" component="h1" gutterBottom>
        优化效果监控面板
      </Typography>

      {/* 错误提示 */}
      {error && (
        <Alert severity="error" sx={{ mb: 4 }}>
          <AlertTitle>错误</AlertTitle>
          {error}
        </Alert>
      )}

      {/* 操作按钮 */}
      <Box sx={{ mb: 6 }}>
        <Button
          variant="contained"
          color="primary"
          onClick={handleRunBenchmark}
          disabled={isRunning}
          startIcon={isRunning ? <CircularProgress size={20} /> : <PlayCircle />}
          sx={{ mr: 2 }}
        >
          {isRunning ? '运行中...' : '运行基准测试'}
        </Button>
        <Button
          variant="outlined"
          onClick={handleReset}
          startIcon={<RotateCcw />}
          sx={{ mr: 2 }}
        >
          重置为默认值
        </Button>
        <Button variant="outlined" onClick={handleBaseline}>
          设置为基线（全关）
        </Button>
      </Box>

      {/* 优化开关状态 */}
      <Paper sx={{ p: 4, mb: 6 }}>
        <Typography variant="h6" gutterBottom>
          优化开关状态
        </Typography>
        <Grid container spacing={3}>
          {flags ? (
            Object.entries(flags).map(([key, value]) => (
              <Grid item xs={12} sm={6} md={4} key={key}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={value as boolean}
                      onChange={(e) => handleFlagChange(key, e.target.checked)}
                      name={key}
                    />
                  }
                  label={flagNames[key] || key}
                />
              </Grid>
            ))
          ) : (
            <CircularProgress />
          )}
        </Grid>
      </Paper>

      {/* 最新测试结果指标 */}
      {latestResult && (
        <div sx={{ mb: 6 }}>
          <Typography variant="h6" gutterBottom>
            最新测试结果 - {formatTime(latestResult.timestamp)}
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="EC 吞吐量"
                value={latestResult.metrics.ec_throughput_mbps.toFixed(1)}
                unit="MB/s"
                improvement={latestResult.comparison?.ec_throughput_improvement}
                icon={<Activity style={{ fontSize: 28 }} />}
                color="primary"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="EC 延迟"
                value={latestResult.metrics.ec_latency_ms.toFixed(2)}
                unit="ms"
                improvement={latestResult.comparison?.ec_latency_improvement}
                icon={<Activity style={{ fontSize: 28 }} />}
                color="secondary"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="Raft 选举时间"
                value={latestResult.metrics.raft_election_time_ms.toFixed(0)}
                unit="ms"
                improvement={latestResult.comparison?.raft_election_improvement}
                icon={<Database style={{ fontSize: 28 }} />}
                color="success"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="KV 缓存命中率"
                value={(latestResult.metrics.kv_cache_hit_rate * 100).toFixed(1)}
                unit="%"
                improvement={latestResult.comparison?.kv_cache_hit_rate_improvement}
                icon={<Memory style={{ fontSize: 28 }} />}
                color="warning"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="KV 读吞吐"
                value={latestResult.metrics.kv_read_throughput_ops.toFixed(0)}
                unit="ops/s"
                improvement={latestResult.comparison?.kv_read_throughput_improvement}
                icon={<Cpu style={{ fontSize: 28 }} />}
                color="primary"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="KV 写吞吐"
                value={latestResult.metrics.kv_write_throughput_ops.toFixed(0)}
                unit="ops/s"
                improvement={latestResult.comparison?.kv_write_throughput_improvement}
                icon={<Cpu style={{ fontSize: 28 }} />}
                color="secondary"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="S3 读吞吐"
                value={latestResult.metrics.s3_read_throughput_mbps.toFixed(1)}
                unit="MB/s"
                improvement={latestResult.comparison?.s3_read_throughput_improvement}
                icon={<Database style={{ fontSize: 28 }} />}
                color="success"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="数据均衡度"
                value={(latestResult.metrics.data_balance_score * 100).toFixed(1)}
                unit="%"
                improvement={latestResult.comparison?.data_balance_improvement}
                icon={<Activity style={{ fontSize: 28 }} />}
                color="warning"
              />
            </Grid>
          </Grid>
        </div>
      )}

      {/* 环境信息 */}
      {latestResult && (
        <Paper sx={{ p: 4, mb: 6 }}>
          <Typography variant="h6" gutterBottom>
            测试环境信息
          </Typography>
          <Grid container spacing={4}>
            <Grid item xs={12} sm={6} md={3}>
              <Typography variant="body2" color="text.secondary">CPU 型号</Typography>
              <Typography variant="body1" fontWeight="bold">
                {latestResult.environment.cpu_model}
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Typography variant="body2" color="text.secondary">CPU 核心数</Typography>
              <Typography variant="body1" fontWeight="bold">
                {latestResult.environment.cpu_cores} 核
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Typography variant="body2" color="text.secondary">内存</Typography>
              <Typography variant="body1" fontWeight="bold">
                {latestResult.environment.memory_gb.toFixed(1)} GB
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Typography variant="body2" color="text.secondary">节点数</Typography>
              <Typography variant="body1" fontWeight="bold">
                {latestResult.environment.node_count} 节点
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Typography variant="body2" color="text.secondary">操作系统</Typography>
              <Typography variant="body1" fontWeight="bold">
                {latestResult.environment.os_version}
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Typography variant="body2" color="text.secondary">Rust 版本</Typography>
              <Typography variant="body1" fontWeight="bold">
                {latestResult.environment.rust_version}
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Typography variant="body2" color="text.secondary">PowerFS 版本</Typography>
              <Typography variant="body1" fontWeight="bold">
                {latestResult.environment.powerfs_version}
              </Typography>
            </Grid>
          </Grid>
        </Paper>
      )}

      {/* 历史测试结果表格 */}
      <Paper sx={{ p: 4 }}>
        <Typography variant="h6" gutterBottom>
          历史测试结果
        </Typography>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>测试 ID</TableCell>
                <TableCell>时间</TableCell>
                <TableCell>EC 吞吐量 (MB/s)</TableCell>
                <TableCell>EC 延迟 (ms)</TableCell>
                <TableCell>Raft 选举 (ms)</TableCell>
                <TableCell>KV 命中率</TableCell>
                <TableCell>持续时间</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {results.map((result) => (
                <TableRow key={result.id} hover>
                  <TableCell>
                    <Tooltip title={result.id}>
                      <Typography noWrap>{result.id.slice(0, 20)}...</Typography>
                    </Tooltip>
                  </TableCell>
                  <TableCell>{formatTime(result.timestamp)}</TableCell>
                  <TableCell>{result.metrics.ec_throughput_mbps.toFixed(1)}</TableCell>
                  <TableCell>{result.metrics.ec_latency_ms.toFixed(2)}</TableCell>
                  <TableCell>{result.metrics.raft_election_time_ms.toFixed(0)}</TableCell>
                  <TableCell>{(result.metrics.kv_cache_hit_rate * 100).toFixed(1)}%</TableCell>
                  <TableCell>{result.duration_seconds} 秒</TableCell>
                </TableRow>
              ))}
              {results.length === 0 && (
                <TableRow>
                  <TableCell colSpan={7} align="center">
                    {isLoading ? (
                      <CircularProgress size={24} />
                    ) : (
                      <Typography color="text.secondary">暂无测试结果</Typography>
                    )}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>
    </div>
  );
};

export default OptimizationDashboard;
