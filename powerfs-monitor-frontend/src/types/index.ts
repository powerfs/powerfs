export type NodeStatus =
  | 'online'
  | 'healthy'
  | 'degraded'
  | 'maintenance'
  | 'isolated'
  | 'offline'
  | 'initializing'
  | 'leader'
  | 'follower'

export interface NodeInfo {
  id: string
  node_type: 'master' | 'volume' | 'filer'
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
  is_leader?: boolean
  raft_term?: number
}

export interface DeviceLocation {
  node_id: string
  device_id: string
  zone: string
  rack?: string
  data_center?: string
}

// Backend DeviceType enum (serde rename_all = "snake_case"):
// Ssd / Nvme / Hdd / Logical. Legacy values kept for backwards compat.
export type DeviceType =
  | 'ssd'
  | 'nvme'
  | 'hdd'
  | 'logical'
  | 'local_file'
  | 'spdk'
  | 'nvmeof'
// Backend DeviceStatus enum (serde rename_all = "snake_case"):
// Online / Offline / Draining / Excluded / ReadOnly. `faulty` is legacy.
export type DeviceStatus =
  | 'online'
  | 'offline'
  | 'excluded'
  | 'draining'
  | 'readonly'
  | 'faulty'
export type DeviceHealth = 'healthy' | 'warning' | 'critical' | 'unknown'

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
  status: 'available' | 'full' | 'read_only' | 'creating' | 'deleting'
  collection: string
  created_at: string
  read_only?: boolean
  replica_placement?: number
  ttl?: number
  disk_type?: string
  compact_status?: number
  append_offset?: number
  // I/O performance counters (cumulative)
  read_ops?: number
  write_ops?: number
  read_bytes?: number
  write_bytes?: number
  read_avg_latency_us?: number
  write_avg_latency_us?: number
  read_p50_latency_us?: number
  read_p99_latency_us?: number
  write_p50_latency_us?: number
  write_p99_latency_us?: number
}

export interface VolumeIoStats {
  volume_id: number
  read_ops: number
  write_ops: number
  read_bytes: number
  write_bytes: number
  read_avg_latency_us: number
  write_avg_latency_us: number
}

export interface FilerNodeInfo {
  node_id: string
  address: string
  grpc_port: number
  http_port: number
  is_healthy: boolean
  leader_count: number
  total_shards: number
}

export interface VolumeServerInfo {
  node: NodeInfo
  volumes: VolumeInfo[]
}

export interface TopologyData {
  masters: NodeInfo[]
  filers: FilerNodeInfo[]
  volume_servers: VolumeServerInfo[]
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
  rule_id: string
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

// ===== Collection management =====

export interface RedundancyInfo {
  mode: string // "replication" | "erasure_coding"
  copies: number | null
  data_shards: number | null
  parity_shards: number | null
  algorithm: string | null
}

export interface StoragePolicyInfo {
  name: string
  redundancy: RedundancyInfo
  min_write_nodes: number
}

export interface VolumeAllocationInfo {
  mode: string // "auto" | "manual" | "hybrid"
  count: number | null
  volume_size: number | null
  volume_ids: number[] | null
  fixed_volume_ids: number[] | null
  auto_count: number | null
}

export interface CollectionInfo {
  name: string
  status: number
  status_name: string
  storage_policy: StoragePolicyInfo | null
  disk_type: string
  capacity_quota_bytes: number
  volume_count: number
  ttl_seconds: number
  created_at: number
  updated_at: number
  description: string
  volume_allocation: VolumeAllocationInfo | null
  excluded_volume_ids: number[]
}

export interface CollectionStats {
  used_bytes: number
  file_count: number
  volume_count: number
  writable_volume_count: number
  read_ops: number
  write_ops: number
  read_bytes: number
  write_bytes: number
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
  filer_address: string
  threads: number
  status: 'mounted' | 'unmounted' | 'error'
  mounted_at: string
  pid?: number
  host?: string
  dirty_chunks?: number
  dirty_bytes?: number
  last_heartbeat?: string
  /** Runtime stats reported by the FUSE client via KeepConnected heartbeat. */
  stats?: ClientStats
}

/** Runtime statistics for a FUSE client (mirrors proto ClientStats). */
export interface ClientStats {
  // Multi-queue scheduler
  data_queue_depth: number
  lease_queue_depth: number
  admin_queue_depth: number
  data_processed_total: number
  lease_processed_total: number
  admin_processed_total: number
  // CircuitBreaker
  cb_closed_count: number
  cb_open_count: number
  cb_half_open_count: number
  cb_trip_total: number
  // WriteCoalescer
  coalescer_dirty_bytes: number
  coalescer_dirty_entries: number
  coalescer_writes_in_total: number
  coalescer_flushes_out_total: number
  // Connection pool
  pool_active_connections: number
  pool_reconnect_total: number
  pool_ping_failures: number
  // Request latency (microseconds)
  read_latency_p50_us: number
  read_latency_p99_us: number
  write_latency_p50_us: number
  write_latency_p99_us: number
  // Lease
  active_leases: number
  lease_renewals_total: number
  lease_expired_total: number
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

// ===== Filer admin bridge types (Monitor 代理 filer /admin/*) =====
// 设计原则: 前端只跟 Monitor 交互, filer admin 由 Monitor 透传。
// 详见 docs/filer-redesign-plan.md。

/**
 * Filer 节点 — 合并 master 注册视角 (gRPC ListFilers) + 心跳视角 (metric_store)。
 * heartbeat_status 是真实健康状态 (受 NODE_HEARTBEAT_TIMEOUT_SECS 控制),
 * registered_healthy 只是 master 静态注册值。
 */
export interface FilerNode {
  node_id: string
  address: string
  http_port: number
  grpc_port: number
  /** master 注册视角的静态健康 (gRPC ListFilers.is_healthy) */
  is_registered: boolean
  registered_healthy: boolean
  leader_count: number
  total_shards: number
  /** 心跳视角的真实健康 ('online' | 'offline') */
  heartbeat_status: string
  /** 距离上次心跳的秒数 */
  last_seen_ago_secs: number
  cpu_usage: number
  mem_usage: number
  disk_usage: number
  uptime: number
}

/**
 * 集群级 shard 视图 — 按 shard_id 聚合多 filer 副本。
 * Phase C 的 /api/filer/cluster/shards 返回此类型。
 */
export interface ClusterShardReplica {
  node_id: string
  is_leader: boolean
  term: number
  commit_index: number
  applied_index: number
  inode_count: number
  write_qps: number
  read_qps: number
}

export interface ClusterShard {
  shard_id: number
  inode_range_start: number
  inode_range_end: number
  replicas: ClusterShardReplica[]
  /** 集群级健康判定 (term 一致 + commit_index 落后 < 阈值) */
  is_healthy: boolean
  /** 不健康时的原因 */
  lag_reason?: string
}

/** cluster/status 单节点条目 (status 为 filer /admin/status 原始透传) */
export interface ClusterStatusNode {
  node_id: string
  status: FilerStatus | null
  error: string | null
}

/** cluster/status 聚合汇总 */
export interface ClusterStatusTotals {
  node_count: number
  reachable: number
  unreachable: number
  total_shards: number
  total_leaders: number
  total_inodes: number
  total_files: number
  total_dirs: number
  all_buckets: string[]
}

/** cluster/status 响应体 */
export interface ClusterStatusResponse {
  nodes: ClusterStatusNode[]
  totals: ClusterStatusTotals
}

/** Balancer 批量操作单个失败条目 */
export interface BatchFailure {
  node_id: string
  error: string
}

/** Balancer 批量操作结果 (start/stop/trigger all) */
export interface BatchResult {
  success: string[]
  failed: BatchFailure[]
  total: number
}

// ===== Master Raft =====
export interface MasterStatus {
  nodes: NodeInfo[]
  leader: NodeInfo | null
  raft_term: number
  total_masters: number
  healthy_masters: number
}

// ===== Runtime config (hot-modify via PUT) =====
export interface CircuitBreakerConfig {
  failure_threshold: number
  recovery_timeout_ms: number
  half_open_max_requests: number
}

export interface CoalescerConfig {
  deadline_ms: number
  min_pending_writes: number
  max_dirty_bytes_per_entry: number
  max_dirty_bytes_total: number
  disabled: boolean
}
