export interface NodeInfo {
  id: string
  address: string
  grpc_port: number
  http_port: number
  status: 'online' | 'offline' | 'warning'
  cpu_usage: number
  mem_usage: number
  disk_usage: number
  network_rx: number
  network_tx: number
  uptime: number
  volume_count: number
}

export interface VolumeInfo {
  id: number
  node_id: string
  size: number
  used: number
  file_count: number
  status: 'available' | 'full' | 'readonly' | 'creating'
  collection: string
  created_at: string
}

export interface KVSessionInfo {
  id: string
  model_name: string
  layer_count: number
  block_count: number
  memory_used: number
  hit_ratio: number
  eviction_count: number
  created_at: string
}

export interface KVBlockInfo {
  block_id: number
  layer_id: number
  num_tokens: number
  size_bytes: number
  fid: string
  last_accessed: string
}

export interface AlertInfo {
  id: string
  name: string
  severity: 'critical' | 'warning' | 'info'
  status: 'firing' | 'pending' | 'resolved'
  source: string
  message: string
  created_at: string
  resolved_at?: string
}

export interface AlertRule {
  id: string
  name: string
  description: string
  enabled: boolean
  severity: 'critical' | 'warning' | 'info'
  condition: {
    type: string
    metric: string
    operator: string
    value: number
    duration: number
  }
  notifications: {
    type: string
    url?: string
    to?: string[]
  }[]
  created_at: string
  updated_at: string
}

export interface ClusterMetrics {
  node_count: number
  volume_count: number
  collection_count: number
  is_leader: boolean
  raft_term: number
  uptime: number
  total_storage: number
  used_storage: number
  file_count: number
}

export interface KVMetrics {
  session_count: number
  block_count: number
  memory_used: number
  hit_ratio: number
  eviction_count: number
  put_count: number
  get_count: number
  avg_latency: number
}

export interface TimeSeriesData {
  time: string
  value: number
}

export type MetricType = 'gauge' | 'counter' | 'histogram'