import type { NodeInfo, VolumeInfo, KVSessionInfo, AlertInfo, AlertRule, ClusterMetrics, KVMetrics, TimeSeriesData, BucketInfo, ObjectInfo, MultipartUploadInfo, S3Metrics, StorageDevice, DataMigrationTask, VolumeScrubStatus, ScrubSummary } from '@/types'

export const mockDevices: StorageDevice[] = [
  {
    device_id: 'dev-1',
    device_type: 'local_file',
    total_capacity: 1099511627776,
    used_space: 751619276800,
    free_space: 347892350976,
    location: { node_id: 'node-1', device_id: 'dev-1', zone: 'zone-a', rack: 'rack-1', data_center: 'dc-1' },
    status: 'online',
    health: 'healthy',
    volume_count: 5,
    last_check: '2026-07-17T06:00:00Z',
  },
  {
    device_id: 'dev-2',
    device_type: 'local_file',
    total_capacity: 2199023255552,
    used_space: 1288490188800,
    free_space: 910533066752,
    location: { node_id: 'node-2', device_id: 'dev-2', zone: 'zone-a', rack: 'rack-1', data_center: 'dc-1' },
    status: 'online',
    health: 'healthy',
    volume_count: 4,
    last_check: '2026-07-17T06:05:00Z',
  },
  {
    device_id: 'dev-3',
    device_type: 'local_file',
    total_capacity: 1099511627776,
    used_space: 977105059840,
    free_space: 122406567936,
    location: { node_id: 'node-3', device_id: 'dev-3', zone: 'zone-b', rack: 'rack-2', data_center: 'dc-1' },
    status: 'online',
    health: 'warning',
    volume_count: 3,
    last_check: '2026-07-17T06:10:00Z',
  },
  {
    device_id: 'dev-4',
    device_type: 'nvmeof',
    total_capacity: 4398046511104,
    used_space: 0,
    free_space: 4398046511104,
    location: { node_id: 'node-2', device_id: 'dev-4', zone: 'zone-b', rack: 'rack-2', data_center: 'dc-1' },
    status: 'offline',
    health: 'critical',
    volume_count: 0,
    last_check: '2026-07-16T22:00:00Z',
  },
  {
    device_id: 'dev-5',
    device_type: 'local_file',
    total_capacity: 549755813888,
    used_space: 329853488320,
    free_space: 219902325568,
    location: { node_id: 'node-1', device_id: 'dev-5', zone: 'zone-a', rack: 'rack-1', data_center: 'dc-1' },
    status: 'draining',
    health: 'warning',
    volume_count: 2,
    last_check: '2026-07-17T05:55:00Z',
  },
]

export const mockMigrationTasks: DataMigrationTask[] = [
  {
    task_id: 'mig-1',
    source_volume_id: 1,
    target_volume_id: 3,
    source_device_id: 'dev-5',
    target_device_id: 'dev-2',
    migration_type: 'drain_device',
    status: 'running',
    progress_percent: 65.5,
    created_at: '2026-07-17T04:00:00Z',
    started_at: '2026-07-17T04:05:00Z',
    data_transferred: 214748364800,
    total_data: 327680000000,
  },
  {
    task_id: 'mig-2',
    source_volume_id: 2,
    source_device_id: 'dev-5',
    target_device_id: 'dev-1',
    migration_type: 'drain_device',
    status: 'pending',
    progress_percent: 0,
    created_at: '2026-07-17T04:00:00Z',
  },
]

export const mockNodes: NodeInfo[] = [
  {
    id: 'node-1',
    node_type: 'master',
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
    node_type: 'volume',
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
    node_type: 'volume',
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
    condition: { metric: 'powerfs_node_disk_usage', operator: '>', value: 80, duration: 300 },
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
    condition: { metric: 'powerfs_node_cpu_usage', operator: '>', value: 85, duration: 120 },
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

export const mockBuckets: BucketInfo[] = [
  { name: 'my-bucket', creation_date: '2026-07-01T10:00:00Z', object_count: 1250, total_size: 21474836480 },
  { name: 'backup-data', creation_date: '2026-07-02T14:00:00Z', object_count: 890, total_size: 16106127360 },
  { name: 'logs', creation_date: '2026-07-03T08:00:00Z', object_count: 2500, total_size: 8053063680 },
  { name: 'media', creation_date: '2026-07-03T16:00:00Z', object_count: 450, total_size: 32212254720 },
]

export const mockObjects: ObjectInfo[] = [
  { key: 'documents/report.pdf', etag: '"abc123"', size: 5242880, last_modified: '2026-07-04T10:00:00Z', storage_class: 'STANDARD' },
  { key: 'images/photo.jpg', etag: '"def456"', size: 10485760, last_modified: '2026-07-04T09:30:00Z', storage_class: 'STANDARD' },
  { key: 'data/file.csv', etag: '"ghi789"', size: 20971520, last_modified: '2026-07-04T08:15:00Z', storage_class: 'STANDARD' },
  { key: 'backup/archive.zip', etag: '"jkl012"', size: 1073741824, last_modified: '2026-07-03T20:00:00Z', storage_class: 'STANDARD' },
  { key: 'logs/server.log', etag: '"mno345"', size: 52428800, last_modified: '2026-07-04T11:00:00Z', storage_class: 'STANDARD' },
]

export const mockMultipartUploads: MultipartUploadInfo[] = [
  { upload_id: 'upload-1', key: 'large-file.bin', bucket: 'my-bucket', initiator: 'user1', creation_date: '2026-07-04T09:00:00Z', part_count: 5, status: 'in_progress' },
  { upload_id: 'upload-2', key: 'backup-full.tar', bucket: 'backup-data', initiator: 'user2', creation_date: '2026-07-04T08:30:00Z', part_count: 12, status: 'in_progress' },
  { upload_id: 'upload-3', key: 'archive-2026.tar', bucket: 'backup-data', initiator: 'user1', creation_date: '2026-07-03T15:00:00Z', part_count: 8, status: 'completed' },
]

export const mockS3Metrics: S3Metrics = {
  bucket_count: 4,
  object_count: 5090,
  total_size: 77846343680,
  active_multipart_uploads: 2,
  put_requests: 15000,
  get_requests: 45000,
  delete_requests: 2000,
}

export const mockScrubStatuses: VolumeScrubStatus[] = [
  {
    volume_id: 1,
    state: 'completed',
    progress: 1.0,
    total_needles: 12500,
    verified_needles: 12500,
    corrupted_needles: 0,
    skipped_needles: 120,
    error_needles: 0,
    last_scrub_at: '2026-07-17T03:00:00Z',
    started_at: '2026-07-17T02:45:00Z',
    completed_at: '2026-07-17T03:00:00Z',
  },
  {
    volume_id: 2,
    state: 'completed',
    progress: 1.0,
    total_needles: 18000,
    verified_needles: 17998,
    corrupted_needles: 2,
    skipped_needles: 85,
    error_needles: 0,
    last_scrub_at: '2026-07-17T03:15:00Z',
    started_at: '2026-07-17T02:50:00Z',
    completed_at: '2026-07-17T03:15:00Z',
    corrupted_needle_ids: [15023, 16782],
  },
  {
    volume_id: 3,
    state: 'running',
    progress: 0.65,
    total_needles: 8000,
    verified_needles: 5200,
    corrupted_needles: 0,
    skipped_needles: 30,
    error_needles: 0,
    started_at: '2026-07-17T07:10:00Z',
  },
  {
    volume_id: 4,
    state: 'idle',
    progress: 0.0,
    total_needles: 10500,
    verified_needles: 0,
    corrupted_needles: 0,
    skipped_needles: 0,
    error_needles: 0,
    last_scrub_at: '2026-07-16T22:00:00Z',
  },
  {
    volume_id: 5,
    state: 'failed',
    progress: 0.3,
    total_needles: 25000,
    verified_needles: 7500,
    corrupted_needles: 15,
    skipped_needles: 200,
    error_needles: 3,
    last_scrub_at: '2026-07-17T04:00:00Z',
    started_at: '2026-07-17T06:00:00Z',
    error: 'I/O error reading volume data: device timeout',
    corrupted_needle_ids: [101, 205, 3402, 5678, 8901, 9999, 10234, 13567, 15678, 17890, 19001, 20123, 21500, 23000, 24100],
  },
  {
    volume_id: 6,
    state: 'completed',
    progress: 1.0,
    total_needles: 5000,
    verified_needles: 5000,
    corrupted_needles: 0,
    skipped_needles: 12,
    error_needles: 0,
    last_scrub_at: '2026-07-17T01:30:00Z',
    started_at: '2026-07-17T01:20:00Z',
    completed_at: '2026-07-17T01:30:00Z',
  },
]

export const mockScrubSummary: ScrubSummary = {
  total_volumes: 8,
  scanned_volumes: 5,
  healthy_volumes: 3,
  corrupted_volumes: 2,
  total_needles: 79000,
  verified_needles: 73198,
  corrupted_needles: 17,
  last_scan_time: '2026-07-17T04:00:00Z',
}