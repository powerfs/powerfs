import type { NodeInfo, VolumeInfo, KVSessionInfo, AlertInfo, AlertRule, ClusterMetrics, KVMetrics, TimeSeriesData } from '@/types'

export const mockNodes: NodeInfo[] = [
  {
    id: 'node-1',
    address: '192.168.1.101',
    grpc_port: 8080,
    http_port: 8081,
    status: 'online',
    cpu_usage: 45.2,
    mem_usage: 62.8,
    disk_usage: 78.5,
    network_rx: 1073741824,
    network_tx: 536870912,
    uptime: 86400,
    volume_count: 5,
  },
  {
    id: 'node-2',
    address: '192.168.1.102',
    grpc_port: 8080,
    http_port: 8081,
    status: 'online',
    cpu_usage: 32.1,
    mem_usage: 55.3,
    disk_usage: 65.2,
    network_rx: 858993459,
    network_tx: 429496729,
    uptime: 72000,
    volume_count: 4,
  },
  {
    id: 'node-3',
    address: '192.168.1.103',
    grpc_port: 8080,
    http_port: 8081,
    status: 'warning',
    cpu_usage: 89.5,
    mem_usage: 92.1,
    disk_usage: 88.3,
    network_rx: 2147483648,
    network_tx: 1073741824,
    uptime: 54000,
    volume_count: 3,
  },
]

export const mockVolumes: VolumeInfo[] = [
  { id: 1, node_id: 'node-1', size: 10737418240, used: 7864320000, file_count: 12500, status: 'available', collection: 'default', created_at: '2026-07-01T10:00:00Z' },
  { id: 2, node_id: 'node-1', size: 10737418240, used: 9227468800, file_count: 18000, status: 'available', collection: 'default', created_at: '2026-07-02T12:00:00Z' },
  { id: 3, node_id: 'node-2', size: 10737418240, used: 5368709120, file_count: 8000, status: 'available', collection: 'default', created_at: '2026-07-01T14:00:00Z' },
  { id: 4, node_id: 'node-2', size: 10737418240, used: 6442450944, file_count: 10500, status: 'available', collection: 'default', created_at: '2026-07-02T08:00:00Z' },
  { id: 5, node_id: 'node-3', size: 10737418240, used: 10240000000, file_count: 25000, status: 'full', collection: 'kv', created_at: '2026-07-03T16:00:00Z' },
  { id: 6, node_id: 'node-1', size: 10737418240, used: 3221225472, file_count: 5000, status: 'available', collection: 'kv', created_at: '2026-07-03T20:00:00Z' },
  { id: 7, node_id: 'node-2', size: 10737418240, used: 1610612736, file_count: 2500, status: 'available', collection: 'kv', created_at: '2026-07-04T08:00:00Z' },
  { id: 8, node_id: 'node-3', size: 10737418240, used: 0, file_count: 0, status: 'creating', collection: 'default', created_at: '2026-07-04T10:00:00Z' },
]

export const mockKVSessions: KVSessionInfo[] = [
  { id: 'session-1', model_name: 'Llama-3-8B', layer_count: 32, block_count: 256, memory_used: 21474836480, hit_ratio: 94.5, eviction_count: 12, created_at: '2026-07-03T10:00:00Z' },
  { id: 'session-2', model_name: 'Qwen-7B', layer_count: 24, block_count: 192, memory_used: 16106127360, hit_ratio: 91.2, eviction_count: 8, created_at: '2026-07-03T14:00:00Z' },
  { id: 'session-3', model_name: 'Mistral-7B', layer_count: 32, block_count: 224, memory_used: 18874368000, hit_ratio: 88.7, eviction_count: 15, created_at: '2026-07-04T08:00:00Z' },
]

export const mockAlerts: AlertInfo[] = [
  { id: 'alert-1', name: '磁盘使用率过高', severity: 'warning', status: 'firing', source: 'node-3', message: '节点磁盘使用率达到88.3%', created_at: '2026-07-04T10:30:00Z' },
  { id: 'alert-2', name: '内存使用率过高', severity: 'warning', status: 'firing', source: 'node-3', message: '节点内存使用率达到92.1%', created_at: '2026-07-04T10:25:00Z' },
  { id: 'alert-3', name: 'CPU使用率过高', severity: 'critical', status: 'firing', source: 'node-3', message: '节点CPU使用率达到89.5%', created_at: '2026-07-04T10:20:00Z' },
  { id: 'alert-4', name: 'Volume已满', severity: 'info', status: 'resolved', source: 'volume-5', message: 'Volume 5已达到容量上限', created_at: '2026-07-04T09:00:00Z', resolved_at: '2026-07-04T09:30:00Z' },
]

export const mockAlertRules: AlertRule[] = [
  {
    id: 'rule-1',
    name: '磁盘使用率过高',
    description: '当节点磁盘使用率超过80%时触发告警',
    enabled: true,
    severity: 'warning',
    condition: { type: 'threshold', metric: 'powerfs_node_disk_usage', operator: '>', value: 80, duration: 300 },
    notifications: [{ type: 'webhook', url: 'https://example.com/webhook' }],
    created_at: '2026-07-01T10:00:00Z',
    updated_at: '2026-07-01T10:00:00Z',
  },
  {
    id: 'rule-2',
    name: 'CPU使用率过高',
    description: '当节点CPU使用率超过85%时触发告警',
    enabled: true,
    severity: 'critical',
    condition: { type: 'threshold', metric: 'powerfs_node_cpu_usage', operator: '>', value: 85, duration: 120 },
    notifications: [{ type: 'webhook', url: 'https://example.com/webhook' }, { type: 'dingtalk', url: 'https://oapi.dingtalk.com/robot/send' }],
    created_at: '2026-07-01T10:00:00Z',
    updated_at: '2026-07-02T14:00:00Z',
  },
]

export const mockClusterMetrics: ClusterMetrics = {
  node_count: 3,
  volume_count: 8,
  collection_count: 2,
  is_leader: true,
  raft_term: 12,
  uptime: 86400,
  total_storage: 85899345920,
  used_storage: 44601671680,
  file_count: 86500,
}

export const mockKVMetrics: KVMetrics = {
  session_count: 3,
  block_count: 672,
  memory_used: 56455331840,
  hit_ratio: 91.5,
  eviction_count: 35,
  put_count: 12500,
  get_count: 89000,
  avg_latency: 2.3,
}

export function generateTimeSeriesData(points: number = 24, baseValue: number = 100, variance: number = 20): TimeSeriesData[] {
  const data: TimeSeriesData[] = []
  const now = Date.now()
  for (let i = points - 1; i >= 0; i--) {
    const time = new Date(now - i * 3600000)
    const value = baseValue + (Math.random() - 0.5) * variance * 2
    data.push({
      time: time.toISOString(),
      value: parseFloat(value.toFixed(2)),
    })
  }
  return data
}