export type NodeStatus =
  | 'online'
  | 'degraded'
  | 'maintenance'
  | 'isolated'
  | 'offline'
  | 'initializing'

export type RaftRole = 'leader' | 'follower'

export interface NodeInfo {
  id: string
  node_type: 'master' | 'volume'
  address: string
  grpc_port: number
  http_port: number
  status: NodeStatus
  cpu_usage: number
  mem_usage: number
  disk_usage: number
  network_rx: number
  network_tx: number
  uptime: number
  volume_count: number
  device_count?: number
  is_leader?: boolean
  raft_role?: RaftRole
}

export interface DeviceLocation {
  node_id: string
  device_id: string
  zone: string
  rack?: string
  data_center?: string
}

export type DeviceType = 'local_file' | 'spdk' | 'nvmeof'
export type DeviceStatus = 'online' | 'offline' | 'excluded' | 'draining' | 'faulty'
export type DeviceHealth = 'healthy' | 'warning' | 'critical'

export interface StorageDevice {
  device_id: string
  device_type: DeviceType
  total_capacity: number
  used_space: number
  free_space: number
  location: DeviceLocation
  status: DeviceStatus
  health?: DeviceHealth
  volume_count?: number
  last_check?: string
}

export type MigrationStatus =
  | 'pending'
  | 'running'
  | 'paused'
  | 'completed'
  | 'failed'
  | 'cancelled'
export type MigrationType = 'volume_migration' | 'drain_device'

export interface DataMigrationTask {
  task_id: string
  source_volume_id: number
  target_volume_id?: number
  source_device_id: string
  target_device_id?: string
  migration_type: MigrationType
  status: MigrationStatus
  progress_percent: number
  created_at: string
  started_at?: string
  completed_at?: string
  error_message?: string
  data_transferred?: number
  total_data?: number
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

export interface KVNamespace {
  id: string
  name: string
  owner_id: string
  created_at: number
  updated_at: number
}

export interface KVAccessKey {
  id: string
  user_id: string
  access_key: string
  status: string
  created_at: string
  last_used_at?: string
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

export interface BucketInfo {
  name: string
  creation_date: string
  object_count: number
  total_size: number
}

export interface ObjectInfo {
  key: string
  etag: string
  size: number
  last_modified: string
  storage_class: string
}

export interface MultipartUploadInfo {
  upload_id: string
  key: string
  bucket: string
  initiator: string
  creation_date: string
  part_count: number
  status: 'in_progress' | 'completed' | 'aborted'
}

export interface S3Metrics {
  bucket_count: number
  object_count: number
  total_size: number
  active_multipart_uploads: number
  put_requests: number
  get_requests: number
  delete_requests: number
}

export interface FuseMount {
  id: string
  mount_point: string
  collection: string
  replication: string
  master: string
  threads: number
  status: 'mounted' | 'unmounted' | 'error'
  mounted_at: string
  pid?: number
  host?: string
  client_type?: string
  dirty_chunks?: number
  dirty_bytes?: number
  last_heartbeat?: string
}

export type ConflictType = 'CreateCreate' | 'WriteWrite' | 'WriteUnlink' | 'DeleteCreate' | 'RenameConflict'

export type ConflictResolution = 'KeepFirst' | 'KeepLast' | 'KeepAll' | 'Merge'

export type MergePolicy =
  | 'LwwTime'
  | 'ContentHash'
  | 'WeightBased'
  | 'KeepAll'
  | 'WritePriority'
  | 'DeletePriority'
  | 'Aggressive'
  | 'Conservative'
  | 'Manual'

export interface ConflictBranch {
  name: string
  client_id: number
  seq: number
  inode: number
  parent_ino: number
  mode: number
  size: number
  mtime: number
  atime: number
  ctime: number
  file_type: number
  symlink_target: string
}

export interface ConflictRecord {
  id: string
  conflict_type: number
  dir_ino: number
  dir_path: string
  base_name: string
  branches: ConflictBranch[]
  create_time: number
  resolved: boolean
  resolved_time: number
  resolution: number
}

export interface ConflictStats {
  total_count: number
  resolved_count: number
  unresolved_count: number
  create_create_count: number
  create_create_resolved: number
  write_write_count: number
  write_write_resolved: number
  write_unlink_count: number
  write_unlink_resolved: number
  delete_create_count: number
  delete_create_resolved: number
  rename_conflict_count: number
  rename_conflict_resolved: number
}

export interface AutoResolveResult {
  success: boolean
  error: string
  resolved_count: number
}

export interface BatchResolveResult {
  success: boolean
  error: string
  resolved_count: number
}

export interface BatchIgnoreResult {
  success: boolean
  error: string
  ignored_count: number
}

export interface S3AccessKey {
  access_key: string
  secret_key: string
  created_at: string
}

export type ScrubState = 'idle' | 'running' | 'paused' | 'completed' | 'failed'

export interface VolumeScrubStatus {
  volume_id: number
  state: ScrubState
  progress: number
  total_needles: number
  verified_needles: number
  corrupted_needles: number
  skipped_needles: number
  error_needles: number
  last_scrub_at?: string
  started_at?: string
  completed_at?: string
  error?: string
  corrupted_needle_ids?: number[]
}

export interface ScrubSummary {
  total_volumes: number
  scanned_volumes: number
  healthy_volumes: number
  corrupted_volumes: number
  total_needles: number
  verified_needles: number
  corrupted_needles: number
  last_scan_time?: string
}

export interface BenchmarkOperation {
  operation: string
  count: number
  duration_ms: number
  ops_per_sec: number
  avg_latency_ms: number
  bandwidth_mbps?: number
}

export interface BenchmarkSummary {
  avg_ops_per_sec?: number
  avg_latency_ms?: number
  avg_bandwidth_mbps?: number
}

export interface BenchmarkReport {
  benchmark: string
  timestamp: string
  config: {
    rounds: number
    iterations_per_round: number
    data_size_bytes?: number
    test_sizes?: number[]
  }
  operations: BenchmarkOperation[]
  summary: Record<string, BenchmarkSummary>
}

export interface BenchmarkResult {
  id: string
  type: 'kv' | 'metadata' | 'fs' | 's3'
  status: 'running' | 'completed' | 'failed'
  started_at: string
  completed_at?: string
  result?: BenchmarkReport
  error?: string
}

// ===== Filer & Shard management =====

export interface FilerStatus {
  shard_count: number
  leader_count: number
  total_inodes: number
  total_files: number
  total_dirs: number
  buckets: string[]
}

export interface ShardDetail {
  shard_id: number
  inode_range_start: number
  inode_range_end: number
  is_leader: boolean
  term: number
  commit_index: number
  applied_index: number
  inode_count: number
  file_count: number
  dir_count: number
  write_qps: number
  read_qps: number
}