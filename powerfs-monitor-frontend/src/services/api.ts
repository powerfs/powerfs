import axios from 'axios'
import type { NodeInfo, VolumeInfo, KVSessionInfo, AlertInfo, AlertRule, ClusterMetrics, KVMetrics, TimeSeriesData, BucketInfo, ObjectInfo, MultipartUploadInfo, S3Metrics, FuseMount, ClientStats, S3AccessKey, KVNamespace, KVAccessKey, ConflictRecord, ConflictStats, AutoResolveResult, BatchResolveResult, BatchIgnoreResult, StorageDevice, DataMigrationTask, VolumeScrubStatus, ScrubSummary, BenchmarkResult, BenchmarkReport, FilerStatus, ShardDetail, FilerNode, ClusterShard, ClusterStatusResponse, BatchResult, TopologyData, CollectionInfo, CollectionStats, MasterStatus, CircuitBreakerConfig, CoalescerConfig, VolumeIoStats } from '@/types'
import { mockNodes, mockVolumes, mockKVSessions, mockAlerts, mockAlertRules, mockClusterMetrics, mockKVMetrics, generateTimeSeriesData, mockBuckets, mockObjects, mockMultipartUploads, mockS3Metrics, mockFuseMounts, mockDevices, mockMigrationTasks, mockScrubStatuses, mockScrubSummary } from '@/utils/mockData'
import { getToken, refreshAccessToken, isPublicUrl, logout } from './auth'

const api = axios.create({
  baseURL: '/api',
  timeout: 10000,
})

export default api

// 请求拦截器：自动注入 Authorization Bearer token
api.interceptors.request.use((config) => {
  const token = getToken()
  if (token && !isPublicUrl(config.url)) {
    config.headers = config.headers ?? {}
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// 响应拦截器：401 时尝试刷新 token，刷新失败则登出并跳转登录
let isRefreshing = false
let refreshSubscribers: Array<(token: string | null) => void> = []

function subscribeTokenRefresh(cb: (token: string | null) => void) {
  refreshSubscribers.push(cb)
}

function onTokenRefreshed(token: string | null) {
  refreshSubscribers.forEach((cb) => cb(token))
  refreshSubscribers = []
}

api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config
    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true

      if (isPublicUrl(originalRequest.url)) {
        return Promise.reject(error)
      }

      if (isRefreshing) {
        return new Promise((resolve, reject) => {
          subscribeTokenRefresh((token) => {
            if (!token) {
              reject(error)
              return
            }
            originalRequest.headers.Authorization = `Bearer ${token}`
            resolve(api(originalRequest))
          })
        })
      }

      isRefreshing = true
      const newToken = await refreshAccessToken()
      isRefreshing = false
      onTokenRefreshed(newToken)

      if (!newToken) {
        // 刷新失败，登出并跳转登录
        logout()
        if (window.location.pathname !== '/login') {
          window.location.href = '/login'
        }
        return Promise.reject(error)
      }

      originalRequest.headers.Authorization = `Bearer ${newToken}`
      return api(originalRequest)
    }
    return Promise.reject(error)
  },
)

let useMock = import.meta.env.VITE_USE_MOCK === 'true'

let mockKVNamespaces: KVNamespace[] = [
  { id: 'ns-1', name: 'default', owner_id: 'user-1', created_at: Date.now() - 86400000, updated_at: Date.now() - 86400000 },
  { id: 'ns-2', name: 'production', owner_id: 'user-1', created_at: Date.now() - 172800000, updated_at: Date.now() - 86400000 },
]

export function setUseMock(value: boolean) {
  useMock = value
}

export async function getTopology(): Promise<TopologyData> {
  const response = await api.get('/topology')
  return response.data.data
}

export async function getClusterMetrics(): Promise<ClusterMetrics> {
  if (useMock) {
    return mockClusterMetrics
  }
  const response = await api.get('/metrics/cluster')
  return response.data.data
}

export async function getKVMetrics(): Promise<KVMetrics> {
  if (useMock) {
    return mockKVMetrics
  }
  const response = await api.get('/metrics/kv')
  return response.data.data
}

export async function getNodes(): Promise<NodeInfo[]> {
  if (useMock) {
    return mockNodes
  }
  const response = await api.get('/metrics/nodes')
  return response.data.data
}

export async function getNode(id: string): Promise<NodeInfo> {
  if (useMock) {
    return mockNodes.find(n => n.id === id) || mockNodes[0]
  }
  const response = await api.get(`/metrics/nodes/${id}`)
  return response.data.data
}

export async function getVolumes(): Promise<VolumeInfo[]> {
  if (useMock) {
    return mockVolumes
  }
  const response = await api.get('/metrics/volumes')
  return response.data.data
}

export async function getVolume(id: number): Promise<VolumeInfo> {
  if (useMock) {
    return mockVolumes.find(v => v.id === id) || mockVolumes[0]
  }
  const response = await api.get(`/metrics/volumes/${id}`)
  return response.data.data
}

export async function getVolumeIo(id: number): Promise<VolumeIoStats> {
  if (useMock) {
    return {
      volume_id: id,
      read_ops: Math.floor(Math.random() * 5000),
      write_ops: Math.floor(Math.random() * 3000),
      read_bytes: Math.floor(Math.random() * 1024 * 1024 * 1024),
      write_bytes: Math.floor(Math.random() * 512 * 1024 * 1024),
      read_avg_latency_us: Math.floor(50 + Math.random() * 200),
      write_avg_latency_us: Math.floor(100 + Math.random() * 400),
    }
  }
  const response = await api.get(`/metrics/volumes/${id}/io`)
  return response.data.data
}

export interface DataPoint {
  timestamp: number
  value: number
}

export interface CapacityHistoryResponse {
  volume_id: number
  data_points: DataPoint[]
}

export interface CapacityProjectionResponse {
  volume_id: number
  current_bytes: number
  projected_bytes: number | null
  hours_ahead: number
  growth_rate_bytes_per_hour: number | null
}

export async function getCapacityHistory(volumeId: number, minutes: number = 1440): Promise<CapacityHistoryResponse> {
  const response = await api.get(`/metrics/volumes/${volumeId}/capacity-history`, { params: { minutes } })
  return response.data.data
}

export async function getCapacityProjection(volumeId: number, hours: number = 24): Promise<CapacityProjectionResponse> {
  const response = await api.get(`/metrics/volumes/${volumeId}/capacity-projection`, { params: { hours } })
  return response.data.data
}

export async function getKVSessions(): Promise<KVSessionInfo[]> {
  if (useMock) {
    return mockKVSessions
  }
  const response = await api.get('/metrics/kv/sessions')
  return response.data.data
}

export async function getKVSession(id: string): Promise<KVSessionInfo> {
  if (useMock) {
    return mockKVSessions.find(s => s.id === id) || mockKVSessions[0]
  }
  const response = await api.get(`/metrics/kv/sessions/${id}`)
  return response.data.data
}

export async function getAlerts(): Promise<AlertInfo[]> {
  if (useMock) {
    return mockAlerts
  }
  const response = await api.get('/alerts')
  return response.data.data
}

export async function getAlertRules(): Promise<AlertRule[]> {
  if (useMock) {
    return mockAlertRules
  }
  const response = await api.get('/alert-rules')
  return response.data.data
}

export async function acknowledgeAlert(id: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.post(`/alerts/${id}/acknowledge`)
}

export async function deleteKVSession(id: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/metrics/kv/sessions/${id}`)
}

export async function deleteNode(id: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/metrics/nodes/${id}`)
}

export async function deleteVolume(id: number): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/metrics/volumes/${id}`)
}

export async function getMetricHistory(metric: string, minutes?: number): Promise<TimeSeriesData[]> {
  if (useMock) {
    const baseValues: Record<string, number> = {
      'powerfs_node_disk_usage': 65,
      'powerfs_node_cpu_usage': 45,
      'powerfs_kv_hit_ratio': 90,
      'powerfs_kv_memory_used': 50,
    }
    return generateTimeSeriesData(24, baseValues[metric] || 100, 20)
  }
  const url = minutes
    ? `/metrics/history/${metric}?minutes=${minutes}`
    : `/metrics/history/${metric}`
  const response = await api.get(url)
  return response.data.data
}

export interface NodeDiskUsageSeries {
  node_id: string
  points: TimeSeriesData[]
}

export async function getClusterDiskUsageBreakdown(minutes: number = 1440): Promise<NodeDiskUsageSeries[]> {
  if (useMock) {
    return []
  }
  const response = await api.get('/metrics/cluster-disk-usage', { params: { minutes } })
  return response.data.data
}

export async function getS3Metrics(): Promise<S3Metrics> {
  if (useMock) {
    return mockS3Metrics
  }
  const response = await api.get('/metrics/s3')
  return response.data.data
}

export async function getBuckets(): Promise<BucketInfo[]> {
  if (useMock) {
    return mockBuckets
  }
  const response = await api.get('/s3/buckets')
  return response.data.data
}

export async function getBucket(name: string): Promise<BucketInfo> {
  if (useMock) {
    return mockBuckets.find(b => b.name === name) || mockBuckets[0]
  }
  const response = await api.get(`/s3/buckets/${name}`)
  return response.data.data
}

export async function createBucket(name: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.post('/s3/buckets', { name })
}

export async function deleteBucket(name: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/s3/buckets/${name}`)
}

export async function getObjects(bucket: string): Promise<ObjectInfo[]> {
  if (useMock) {
    return mockObjects
  }
  const response = await api.get(`/s3/buckets/${bucket}/objects`)
  return response.data.data
}

export async function deleteObject(bucket: string, key: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/s3/buckets/${bucket}/objects/${encodeURIComponent(key)}`)
}

export async function uploadObject(bucket: string, key: string, file: File): Promise<void> {
  if (useMock) {
    return
  }
  const formData = new FormData()
  formData.append('key', key)
  formData.append('file', file)
  await api.post(`/s3/buckets/${bucket}/objects`, formData, {
    headers: { 'Content-Type': undefined },
  })
}

export async function downloadObject(bucket: string, key: string): Promise<void> {
  if (useMock) {
    return
  }
  const response = await api.get(`/s3/buckets/${bucket}/objects/${encodeURIComponent(key)}/download`, {
    responseType: 'blob',
  })
  const blob = response.data
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = key
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(url)
}

export async function getMultipartUploads(bucket?: string): Promise<MultipartUploadInfo[]> {
  if (useMock) {
    if (bucket) {
      return mockMultipartUploads.filter(u => u.bucket === bucket)
    }
    return mockMultipartUploads
  }
  const url = bucket ? `/s3/multipart-uploads?bucket=${bucket}` : '/s3/multipart-uploads'
  const response = await api.get(url)
  return response.data.data
}

export async function abortMultipartUpload(bucket: string, key: string, uploadId: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/s3/buckets/${bucket}/objects/${encodeURIComponent(key)}?uploadId=${uploadId}`)
}

export async function getS3AccessKeys(): Promise<S3AccessKey[]> {
  if (useMock) {
    return [{ access_key: 'powerfs', secret_key: 'powerfs123', created_at: new Date().toISOString() }]
  }
  const response = await api.get('/s3/keys')
  return response.data.data
}

export async function createS3AccessKey(accessKey: string, secretKey: string): Promise<S3AccessKey> {
  if (useMock) {
    return { access_key: accessKey, secret_key: secretKey, created_at: new Date().toISOString() }
  }
  const response = await api.post('/s3/keys', { access_key: accessKey, secret_key: secretKey })
  return response.data.data
}

export async function deleteS3AccessKey(accessKey: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/s3/keys/${encodeURIComponent(accessKey)}`)
}

export async function getFuseMounts(): Promise<FuseMount[]> {
  if (useMock) {
    return mockFuseMounts
  }
  const response = await api.get('/fuse/mounts')
  return response.data.data
}

export async function getFuseClients(): Promise<FuseMount[]> {
  if (useMock) {
    return mockFuseMounts
  }
  const response = await api.get('/fuse/clients')
  return response.data.data
}

export async function getFuseClientStats(clientId: string): Promise<ClientStats | null> {
  if (useMock) {
    return null
  }
  try {
    const response = await api.get(`/fuse/clients/${clientId}/stats`)
    return response.data.data
  } catch {
    return null
  }
}

export async function createFuseMount(mount: {
  mount_point: string
  collection: string
  replication: string
  filer_address: string
  threads: number
}): Promise<FuseMount> {
  if (useMock) {
    return {
      id: 'mock-id',
      ...mount,
      status: 'mounted',
      mounted_at: new Date().toISOString(),
    }
  }
  const response = await api.post('/fuse/mounts', mount)
  return response.data.data
}

export async function deleteFuseMount(id: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/fuse/mounts/${id}`)
}

// ===== Conflict management =====

export async function getConflicts(params?: {
  dir_path?: string
  dir_ino?: number
  unresolved_only?: boolean
}): Promise<ConflictRecord[]> {
  if (useMock) {
    return []
  }
  const response = await api.get('/conflicts', { params })
  return response.data.data
}

export async function getConflictStats(params?: {
  dir_path?: string
  dir_ino?: number
  recursive?: boolean
}): Promise<ConflictStats> {
  if (useMock) {
    return {
      total_count: 0, resolved_count: 0, unresolved_count: 0,
      create_create_count: 0, create_create_resolved: 0,
      write_write_count: 0, write_write_resolved: 0,
      write_unlink_count: 0, write_unlink_resolved: 0,
      delete_create_count: 0, delete_create_resolved: 0,
      rename_conflict_count: 0, rename_conflict_resolved: 0,
    }
  }
  const response = await api.get('/conflicts/stats', { params })
  return response.data.data
}

export async function resolveConflict(params: {
  conflict_id: string
  dir_path?: string
  dir_ino?: number
  resolution: number
}): Promise<void> {
  if (useMock) {
    return
  }
  await api.post('/conflicts/resolve', params)
}

export async function autoResolveConflicts(params: {
  dir_path?: string
  dir_ino?: number
  policy: number
}): Promise<AutoResolveResult> {
  if (useMock) {
    return { success: true, error: '', resolved_count: 0 }
  }
  const response = await api.post('/conflicts/auto-resolve', params)
  return response.data.data
}

export async function batchResolveConflicts(params: {
  dir_path?: string
  dir_ino?: number
  recursive?: boolean
  conflict_type?: number
  policy: number
}): Promise<BatchResolveResult> {
  if (useMock) {
    return { success: true, error: '', resolved_count: 0 }
  }
  const response = await api.post('/conflicts/batch-resolve', params)
  return response.data.data
}

export async function batchIgnoreConflicts(params: {
  dir_path?: string
  dir_ino?: number
  conflict_type?: number
}): Promise<BatchIgnoreResult> {
  if (useMock) {
    return { success: true, error: '', ignored_count: 0 }
  }
  const response = await api.post('/conflicts/batch-ignore', params)
  return response.data.data
}

export async function createKVNamespace(name: string): Promise<void> {
  if (useMock) {
    const newNamespace: KVNamespace = {
      id: `ns-${Date.now()}`,
      name,
      owner_id: 'user-1',
      created_at: Date.now(),
      updated_at: Date.now(),
    }
    mockKVNamespaces.push(newNamespace)
    return
  }
  await api.post('/kv/namespaces', { name })
}

export async function listKVNamespaces(): Promise<KVNamespace[]> {
  if (useMock) {
    return mockKVNamespaces
  }
  const response = await api.get('/kv/namespaces')
  return response.data.data
}

export async function getKVNamespace(id: string): Promise<KVNamespace> {
  if (useMock) {
    const ns = mockKVNamespaces.find(n => n.id === id)
    return ns || { id, name: 'default', owner_id: 'user-1', created_at: Date.now(), updated_at: Date.now() }
  }
  const response = await api.get(`/kv/namespaces/${id}`)
  return response.data.data
}

export async function deleteKVNamespace(id: string): Promise<void> {
  if (useMock) {
    mockKVNamespaces = mockKVNamespaces.filter(n => n.id !== id)
    return
  }
  await api.delete(`/kv/namespaces/${id}`)
}

export async function createKVKey(): Promise<{ id: string; user_id: string; access_key: string; api_key: string; status: string; created_at: string }> {
  if (useMock) {
    return {
      id: 'key-1',
      user_id: 'user-1',
      access_key: 'mock-access-key',
      api_key: 'pak_mock-access-key_mock-secret-key',
      status: 'active',
      created_at: new Date().toISOString(),
    }
  }
  const response = await api.post('/kv/keys')
  return response.data.data
}

export async function listKVKeys(): Promise<KVAccessKey[]> {
  if (useMock) {
    return [
      { id: 'key-1', user_id: 'user-1', access_key: 'mock-access-key', status: 'active', created_at: new Date(Date.now() - 86400000).toISOString() },
      { id: 'key-2', user_id: 'user-1', access_key: 'mock-access-key-2', status: 'inactive', created_at: new Date(Date.now() - 172800000).toISOString() },
    ]
  }
  const response = await api.get('/kv/keys')
  return response.data.data
}

export async function deleteKVKey(id: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/kv/keys/${id}`)
}

export async function getDevices(nodeId?: string): Promise<StorageDevice[]> {
  if (useMock) {
    if (nodeId) {
      return mockDevices.filter(d => d.location.node_id === nodeId)
    }
    return mockDevices
  }
  const url = nodeId ? `/storage/devices?node_id=${nodeId}` : '/storage/devices'
  const response = await api.get(url)
  return response.data.data
}

export async function getDevice(deviceId: string): Promise<StorageDevice> {
  if (useMock) {
    return mockDevices.find(d => d.device_id === deviceId) || mockDevices[0]
  }
  const response = await api.get(`/storage/devices/${deviceId}`)
  return response.data.data
}

export async function excludeDevice(deviceId: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.post(`/storage/devices/${deviceId}/exclude`)
}

export async function restoreDevice(deviceId: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.post(`/storage/devices/${deviceId}/restore`)
}

export async function drainDevice(deviceId: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.post(`/storage/devices/${deviceId}/drain`)
}

export async function getMigrationTasks(deviceId?: string): Promise<DataMigrationTask[]> {
  if (useMock) {
    if (deviceId) {
      return mockMigrationTasks.filter(t => t.source_device_id === deviceId || t.target_device_id === deviceId)
    }
    return mockMigrationTasks
  }
  const url = deviceId ? `/storage/migrations?device_id=${deviceId}` : '/storage/migrations'
  const response = await api.get(url)
  return response.data.data
}

export async function cancelMigration(taskId: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.post(`/storage/migrations/${taskId}/cancel`)
}

export async function pauseMigration(taskId: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.post(`/storage/migrations/${taskId}/pause`)
}

export async function resumeMigration(taskId: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.post(`/storage/migrations/${taskId}/resume`)
}

export async function getScrubSummary(): Promise<ScrubSummary> {
  if (useMock) {
    return mockScrubSummary
  }
  const response = await api.get('/bitrot/scrub/summary')
  return response.data.data
}

export async function getScrubStatuses(): Promise<VolumeScrubStatus[]> {
  if (useMock) {
    return mockScrubStatuses
  }
  const response = await api.get('/bitrot/scrub/statuses')
  return response.data.data
}

export async function getScrubStatus(volumeId: number): Promise<VolumeScrubStatus> {
  if (useMock) {
    return mockScrubStatuses.find(s => s.volume_id === volumeId) || mockScrubStatuses[0]
  }
  const response = await api.get(`/bitrot/scrub/statuses/${volumeId}`)
  return response.data.data
}

export async function triggerScrubVolume(volumeId: number): Promise<void> {
  if (useMock) {
    return
  }
  await api.post(`/bitrot/scrub/trigger/${volumeId}`)
}

export async function triggerScrubAll(): Promise<void> {
  if (useMock) {
    return
  }
  await api.post('/bitrot/scrub/trigger-all')
}

export async function getBenchmarkResults(): Promise<BenchmarkResult[]> {
  if (useMock) {
    return [
      {
        id: 'kv-1',
        type: 'kv',
        status: 'completed',
        started_at: new Date(Date.now() - 3600000).toISOString(),
        completed_at: new Date(Date.now() - 3500000).toISOString(),
        result: {
          benchmark: 'kv',
          timestamp: new Date().toISOString(),
          config: { rounds: 3, iterations_per_round: 10000, data_size_bytes: 1024 },
          operations: [],
          summary: {
            PUT: { avg_ops_per_sec: 15024.31, avg_latency_ms: 0.0666 },
            GET: { avg_ops_per_sec: 4950599.12, avg_latency_ms: 0.0002 },
            EXISTS: { avg_ops_per_sec: 7605315.91, avg_latency_ms: 0.0001 },
            LIST: { avg_ops_per_sec: 475.15, avg_latency_ms: 2.1047 },
            DELETE: { avg_ops_per_sec: 4791583.64, avg_latency_ms: 0.0002 },
          },
        },
      },
      {
        id: 'metadata-1',
        type: 'metadata',
        status: 'completed',
        started_at: new Date(Date.now() - 3400000).toISOString(),
        completed_at: new Date(Date.now() - 3300000).toISOString(),
        result: {
          benchmark: 'metadata',
          timestamp: new Date().toISOString(),
          config: { rounds: 2, iterations_per_round: 500 },
          operations: [],
          summary: {
            CREATE_DIR: { avg_ops_per_sec: 43419.62, avg_latency_ms: 0.0231 },
            CREATE_FILE: { avg_ops_per_sec: 10709.68, avg_latency_ms: 0.0934 },
            READ_FILE: { avg_ops_per_sec: 58591.53, avg_latency_ms: 0.0171 },
            RENAME: { avg_ops_per_sec: 43904.07, avg_latency_ms: 0.0239 },
            LIST_DIR: { avg_ops_per_sec: 91832.51, avg_latency_ms: 0.0109 },
            DELETE: { avg_ops_per_sec: 95914.32, avg_latency_ms: 0.0104 },
          },
        },
      },
      {
        id: 'fs-1',
        type: 'fs',
        status: 'completed',
        started_at: new Date(Date.now() - 3200000).toISOString(),
        completed_at: new Date(Date.now() - 3100000).toISOString(),
        result: {
          benchmark: 'fs',
          timestamp: new Date().toISOString(),
          config: { rounds: 2, iterations_per_round: 100, test_sizes: [65536, 262144, 1048576] },
          operations: [],
          summary: {
            WRITE_64KB: { avg_bandwidth_mbps: 7919.42, avg_latency_ms: 0.0667 },
            READ_64KB: { avg_bandwidth_mbps: 26387.42, avg_latency_ms: 0.0199 },
            WRITE_256KB: { avg_bandwidth_mbps: 10190.35, avg_latency_ms: 0.2061 },
            READ_256KB: { avg_bandwidth_mbps: 32530.24, avg_latency_ms: 0.0654 },
            WRITE_1024KB: { avg_bandwidth_mbps: 9896.00, avg_latency_ms: 0.8481 },
            READ_1024KB: { avg_bandwidth_mbps: 29832.40, avg_latency_ms: 0.2826 },
            CREATE_SMALL: { avg_ops_per_sec: 42507.82, avg_latency_ms: 0.0235 },
            DELETE_SMALL: { avg_ops_per_sec: 109030.11, avg_latency_ms: 0.0092 },
          },
        },
      },
    ]
  }
  const response = await api.get('/benchmarks')
  return response.data.data
}

export async function getBenchmarkReport(type: string): Promise<BenchmarkReport> {
  if (useMock) {
    const mockReports: Record<string, BenchmarkReport> = {
      kv: {
        benchmark: 'kv',
        timestamp: new Date().toISOString(),
        config: { rounds: 3, iterations_per_round: 10000, data_size_bytes: 1024 },
        operations: [],
        summary: {
          PUT: { avg_ops_per_sec: 15024.31, avg_latency_ms: 0.0666 },
          GET: { avg_ops_per_sec: 4950599.12, avg_latency_ms: 0.0002 },
          EXISTS: { avg_ops_per_sec: 7605315.91, avg_latency_ms: 0.0001 },
          LIST: { avg_ops_per_sec: 475.15, avg_latency_ms: 2.1047 },
          DELETE: { avg_ops_per_sec: 4791583.64, avg_latency_ms: 0.0002 },
        },
      },
      metadata: {
        benchmark: 'metadata',
        timestamp: new Date().toISOString(),
        config: { rounds: 2, iterations_per_round: 500 },
        operations: [],
        summary: {
          CREATE_DIR: { avg_ops_per_sec: 43419.62, avg_latency_ms: 0.0231 },
          CREATE_FILE: { avg_ops_per_sec: 10709.68, avg_latency_ms: 0.0934 },
          READ_FILE: { avg_ops_per_sec: 58591.53, avg_latency_ms: 0.0171 },
          RENAME: { avg_ops_per_sec: 43904.07, avg_latency_ms: 0.0239 },
          LIST_DIR: { avg_ops_per_sec: 91832.51, avg_latency_ms: 0.0109 },
          DELETE: { avg_ops_per_sec: 95914.32, avg_latency_ms: 0.0104 },
        },
      },
      fs: {
        benchmark: 'fs',
        timestamp: new Date().toISOString(),
        config: { rounds: 2, iterations_per_round: 100, test_sizes: [65536, 262144, 1048576] },
        operations: [],
        summary: {
          WRITE_64KB: { avg_bandwidth_mbps: 7919.42, avg_latency_ms: 0.0667 },
          READ_64KB: { avg_bandwidth_mbps: 26387.42, avg_latency_ms: 0.0199 },
          WRITE_256KB: { avg_bandwidth_mbps: 10190.35, avg_latency_ms: 0.2061 },
          READ_256KB: { avg_bandwidth_mbps: 32530.24, avg_latency_ms: 0.0654 },
          WRITE_1024KB: { avg_bandwidth_mbps: 9896.00, avg_latency_ms: 0.8481 },
          READ_1024KB: { avg_bandwidth_mbps: 29832.40, avg_latency_ms: 0.2826 },
          CREATE_SMALL: { avg_ops_per_sec: 42507.82, avg_latency_ms: 0.0235 },
          DELETE_SMALL: { avg_ops_per_sec: 109030.11, avg_latency_ms: 0.0092 },
        },
      },
    }
    return mockReports[type] || mockReports.kv
  }
  const response = await api.get(`/benchmarks/${type}`)
  return response.data.data
}

export async function runBenchmark(type: 'kv' | 'metadata' | 'fs' | 's3'): Promise<BenchmarkResult> {
  if (useMock) {
    return {
      id: `${type}-${Date.now()}`,
      type,
      status: 'completed',
      started_at: new Date().toISOString(),
      completed_at: new Date().toISOString(),
      result: await getBenchmarkReport(type),
    }
  }
  const response = await api.post(`/benchmarks/${type}/run`)
  return response.data.data
}

export async function getBenchmarkReportById(id: string): Promise<BenchmarkResult> {
  if (useMock) {
    const results = await getBenchmarkResults()
    return results.find(r => r.id === id) || results[0]
  }
  const response = await api.get(`/benchmarks/report/${id}`)
  return response.data.data
}

// ===== Filer admin bridge (Monitor 代理 filer /admin/*) =====
// 设计原则 (见 docs/filer-redesign-plan.md): 前端只跟 Monitor 交互。
// 所有 filer /admin/* 调用由 Monitor 通过 filer_admin_client 透传,
// Monitor 统一返回 { code, message, data } ApiResponse 包装,
// 因此前端读取 response.data.data (而非旧 nginx 反代的 response.data)。
// 所有 /api/filer/* 接口 (含读操作) 都要求 admin 权限。

/**
 * 集群 filer 节点列表 — 合并 gRPC ListFilers (注册视角) + metric_store (心跳视角)。
 * 推荐轮询间隔: 10s。
 */
export async function getFilerNodes(): Promise<FilerNode[]> {
  if (useMock) {
    return [
      {
        node_id: 'filer-1', address: '127.0.0.1', http_port: 8888, grpc_port: 8889,
        is_registered: true, registered_healthy: true,
        leader_count: 4, total_shards: 4,
        heartbeat_status: 'online', last_seen_ago_secs: 1,
        cpu_usage: 12.5, mem_usage: 34.2, disk_usage: 45.0, uptime: 86400,
      },
    ]
  }
  const response = await api.get('/filer/nodes')
  return response.data.data
}

/** 单节点详细状态 (filer /admin/status 透传)。推荐轮询: 不缓存, 要实时。 */
export async function getFilerNodeStatus(nodeId: string): Promise<FilerStatus> {
  if (useMock) {
    return {
      shard_count: 4,
      leader_count: 4,
      total_inodes: 128,
      total_files: 96,
      total_dirs: 32,
      buckets: ['test-bucket', 'prod-data'],
    }
  }
  const response = await api.get(`/filer/nodes/${nodeId}/status`)
  return response.data.data
}

/** 单节点 shard 列表 (filer /admin/shards 透传)。 */
export async function getFilerNodeShards(nodeId: string): Promise<ShardDetail[]> {
  if (useMock) {
    return [
      { shard_id: 0, inode_range_start: 0, inode_range_end: 1000000, is_leader: true, term: 2, commit_index: 15, applied_index: 15, inode_count: 32, file_count: 24, dir_count: 8, write_qps: 120, read_qps: 480 },
      { shard_id: 1, inode_range_start: 1000000, inode_range_end: 2000000, is_leader: true, term: 2, commit_index: 12, applied_index: 12, inode_count: 48, file_count: 36, dir_count: 12, write_qps: 90, read_qps: 360 },
      { shard_id: 2, inode_range_start: 2000000, inode_range_end: 3000000, is_leader: true, term: 2, commit_index: 8, applied_index: 8, inode_count: 24, file_count: 18, dir_count: 6, write_qps: 60, read_qps: 240 },
      { shard_id: 3, inode_range_start: 3000000, inode_range_end: Number.MAX_SAFE_INTEGER, is_leader: true, term: 2, commit_index: 5, applied_index: 5, inode_count: 24, file_count: 18, dir_count: 6, write_qps: 30, read_qps: 120 },
    ]
  }
  const response = await api.get(`/filer/nodes/${nodeId}/shards`)
  return response.data.data
}

/** 单 shard 详情 (filer /admin/shards/:id 透传)。 */
export async function getFilerNodeShard(nodeId: string, shardId: number | string): Promise<ShardDetail> {
  if (useMock) {
    const shards = await getFilerNodeShards(nodeId)
    return shards.find(s => s.shard_id === Number(shardId)) || shards[0]
  }
  const response = await api.get(`/filer/nodes/${nodeId}/shards/${shardId}`)
  return response.data.data
}

// ===== Shard Balancer API (per-node, Monitor 透传) =====

export interface SchedulerStatus {
  is_running: boolean
  last_check_time: number
  total_migrations: number
  successful_migrations: number
  failed_migrations: number
  node_count: number
  shard_count: number
  leader_distribution: Record<string, number>
}

export interface SchedulerConfig {
  check_interval: number
  max_transfers_per_round: number
  transfer_interval: number
  cooldown_periods: number
  leader_imbalance_threshold: number
  cpu_threshold: number
  memory_threshold: number
  disk_threshold: number
}

/** 单节点 balancer 状态 (filer /admin/balancer/status 透传)。推荐轮询: 5s。 */
export async function getFilerNodeBalancerStatus(nodeId: string): Promise<SchedulerStatus> {
  if (useMock) {
    return {
      is_running: true,
      last_check_time: Date.now() / 1000 | 0,
      total_migrations: 5,
      successful_migrations: 5,
      failed_migrations: 0,
      node_count: 3,
      shard_count: 4,
      leader_distribution: {
        '127.0.0.1:8889': 2,
        '127.0.0.1:8890': 1,
        '127.0.0.1:8891': 1,
      },
    }
  }
  const response = await api.get(`/filer/nodes/${nodeId}/balancer/status`)
  return response.data.data
}

export async function startFilerNodeBalancer(nodeId: string): Promise<void> {
  if (useMock) return
  await api.post(`/filer/nodes/${nodeId}/balancer/start`)
}

export async function stopFilerNodeBalancer(nodeId: string): Promise<void> {
  if (useMock) return
  await api.post(`/filer/nodes/${nodeId}/balancer/stop`)
}

export async function triggerFilerNodeBalancer(nodeId: string): Promise<void> {
  if (useMock) return
  await api.post(`/filer/nodes/${nodeId}/balancer/trigger`)
}

export async function getFilerNodeBalancerConfig(nodeId: string): Promise<SchedulerConfig> {
  if (useMock) {
    return {
      check_interval: 60,
      max_transfers_per_round: 2,
      transfer_interval: 10,
      cooldown_periods: 5,
      leader_imbalance_threshold: 1.5,
      cpu_threshold: 0.8,
      memory_threshold: 0.85,
      disk_threshold: 0.1,
    }
  }
  const response = await api.get(`/filer/nodes/${nodeId}/balancer/config`)
  return response.data.data
}

export async function updateFilerNodeBalancerConfig(nodeId: string, config: SchedulerConfig): Promise<void> {
  if (useMock) return
  await api.put(`/filer/nodes/${nodeId}/balancer/config`, config)
}

// ===== Filer cluster aggregation (Phase C) =====
// 集群聚合端点: Monitor 并发调所有 filer, 缓存 5s (见 docs/filer-redesign-plan.md 决策 3)。
// 推荐轮询: cluster/status 5s, cluster/shards 15s。

/** 集群级状态聚合 (并发调所有 filer /admin/status)。推荐轮询: 5s。 */
export async function getFilerClusterStatus(): Promise<ClusterStatusResponse> {
  if (useMock) {
    return {
      nodes: [
        {
          node_id: 'filer-1',
          status: { shard_count: 4, leader_count: 4, total_inodes: 128, total_files: 96, total_dirs: 32, buckets: ['test-bucket'] },
          error: null,
        },
      ],
      totals: {
        node_count: 1, reachable: 1, unreachable: 0,
        total_shards: 4, total_leaders: 4, total_inodes: 128, total_files: 96, total_dirs: 32,
        all_buckets: ['test-bucket'],
      },
    }
  }
  const response = await api.get('/filer/cluster/status')
  return response.data.data
}

/** 集群级 shard 聚合 (按 shard_id 聚合多副本 + 健康判定)。推荐轮询: 15s。 */
export async function getFilerClusterShards(): Promise<ClusterShard[]> {
  if (useMock) {
    return [
      {
        shard_id: 0, inode_range_start: 0, inode_range_end: 1000000,
        replicas: [
          { node_id: 'filer-1', is_leader: true, term: 2, commit_index: 15, applied_index: 15, inode_count: 32, write_qps: 120, read_qps: 480 },
          { node_id: 'filer-2', is_leader: false, term: 2, commit_index: 15, applied_index: 15, inode_count: 32, write_qps: 0, read_qps: 30 },
        ],
        is_healthy: true,
      },
    ]
  }
  const response = await api.get('/filer/cluster/shards')
  return response.data.data
}

/** Balancer 批量启动 (并发调所有 filer /admin/balancer/start)。写后失效缓存。 */
export async function startAllBalancers(): Promise<BatchResult> {
  if (useMock) return { success: ['filer-1'], failed: [], total: 1 }
  const response = await api.post('/filer/balancer/start-all')
  return response.data.data
}

/** Balancer 批量停止 (并发调所有 filer /admin/balancer/stop)。写后失效缓存。 */
export async function stopAllBalancers(): Promise<BatchResult> {
  if (useMock) return { success: ['filer-1'], failed: [], total: 1 }
  const response = await api.post('/filer/balancer/stop-all')
  return response.data.data
}

/** Balancer 批量触发 (并发调所有 filer /admin/balancer/trigger)。写后失效缓存。 */
export async function triggerAllBalancers(): Promise<BatchResult> {
  if (useMock) return { success: ['filer-1'], failed: [], total: 1 }
  const response = await api.post('/filer/balancer/trigger-all')
  return response.data.data
}

// ===== 向后兼容封装 (过渡期, 调用第一个 filer 节点) =====
// 这些函数保留旧签名, 内部 fetch 节点列表后取第一个节点调用新 per-node 函数。
// 新代码应直接使用上面的 per-node 函数并显式传 nodeId。

/**
 * 取第一个在线 filer 节点 ID; 没有节点时返回 null。
 * 用于向后兼容封装。
 */
async function pickFirstFilerNodeId(): Promise<string | null> {
  const nodes = await getFilerNodes()
  if (nodes.length === 0) return null
  // 优先选心跳在线的节点; 都离线时退而取第一个
  // 存活状态匹配: online/healthy/leader/follower (项目硬约束)
  const ALIVE_STATES = ['online', 'healthy', 'leader', 'follower']
  const online = nodes.find(n => ALIVE_STATES.includes(n.heartbeat_status))
  return (online ?? nodes[0]).node_id
}

/** @deprecated 使用 getFilerNodeStatus(nodeId) */
export async function getFilerStatus(): Promise<FilerStatus> {
  if (useMock) return getFilerNodeStatus('')
  const nodeId = await pickFirstFilerNodeId()
  if (!nodeId) {
    return { shard_count: 0, leader_count: 0, total_inodes: 0, total_files: 0, total_dirs: 0, buckets: [] }
  }
  return getFilerNodeStatus(nodeId)
}

/** @deprecated 使用 getFilerNodeShards(nodeId) */
export async function getShards(): Promise<ShardDetail[]> {
  if (useMock) return getFilerNodeShards('')
  const nodeId = await pickFirstFilerNodeId()
  if (!nodeId) return []
  return getFilerNodeShards(nodeId)
}

/** @deprecated 使用 getFilerNodeShard(nodeId, shardId) */
export async function getShardDetail(id: number): Promise<ShardDetail> {
  if (useMock) return getFilerNodeShard('', id)
  const nodeId = await pickFirstFilerNodeId()
  if (!nodeId) {
    return { shard_id: id, inode_range_start: 0, inode_range_end: 0, is_leader: false, term: 0, commit_index: 0, applied_index: 0, inode_count: 0, file_count: 0, dir_count: 0, write_qps: 0, read_qps: 0 }
  }
  return getFilerNodeShard(nodeId, id)
}

/** @deprecated 使用 getFilerNodeBalancerStatus(nodeId) */
export async function getBalancerStatus(): Promise<SchedulerStatus> {
  if (useMock) return getFilerNodeBalancerStatus('')
  const nodeId = await pickFirstFilerNodeId()
  if (!nodeId) {
    return { is_running: false, last_check_time: 0, total_migrations: 0, successful_migrations: 0, failed_migrations: 0, node_count: 0, shard_count: 0, leader_distribution: {} }
  }
  return getFilerNodeBalancerStatus(nodeId)
}

/** @deprecated 使用 startFilerNodeBalancer(nodeId) */
export async function startBalancer(): Promise<void> {
  if (useMock) return
  const nodeId = await pickFirstFilerNodeId()
  if (nodeId) await startFilerNodeBalancer(nodeId)
}

/** @deprecated 使用 stopFilerNodeBalancer(nodeId) */
export async function stopBalancer(): Promise<void> {
  if (useMock) return
  const nodeId = await pickFirstFilerNodeId()
  if (nodeId) await stopFilerNodeBalancer(nodeId)
}

/** @deprecated 使用 triggerFilerNodeBalancer(nodeId) */
export async function triggerBalance(): Promise<void> {
  if (useMock) return
  const nodeId = await pickFirstFilerNodeId()
  if (nodeId) await triggerFilerNodeBalancer(nodeId)
}

/** @deprecated 使用 getFilerNodeBalancerConfig(nodeId) */
export async function getBalancerConfig(): Promise<SchedulerConfig> {
  if (useMock) return getFilerNodeBalancerConfig('')
  const nodeId = await pickFirstFilerNodeId()
  if (!nodeId) {
    return { check_interval: 60, max_transfers_per_round: 2, transfer_interval: 10, cooldown_periods: 5, leader_imbalance_threshold: 1.5, cpu_threshold: 0.8, memory_threshold: 0.85, disk_threshold: 0.1 }
  }
  return getFilerNodeBalancerConfig(nodeId)
}

/** @deprecated 使用 updateFilerNodeBalancerConfig(nodeId, config) */
export async function setBalancerConfig(config: SchedulerConfig): Promise<void> {
  if (useMock) return
  const nodeId = await pickFirstFilerNodeId()
  if (nodeId) await updateFilerNodeBalancerConfig(nodeId, config)
}

// ===== Collection management =====

export interface RedundancyParams {
  mode: 'replication' | 'erasure_coding'
  copies?: number
  data_shards?: number
  parity_shards?: number
  algorithm?: string
}

export interface StoragePolicyParams {
  name?: string
  redundancy: RedundancyParams
  min_write_nodes?: number
}

export interface VolumeAllocationParams {
  mode: 'auto' | 'manual' | 'hybrid'
  count?: number
  volume_size?: number
  volume_ids?: number[]
  fixed_volume_ids?: number[]
  auto_count?: number
}

export interface CreateCollectionParams {
  name: string
  status?: number
  storage_policy?: StoragePolicyParams
  disk_type?: string
  capacity_quota_bytes?: number
  volume_count?: number
  ttl_seconds?: number
  description?: string
  volume_allocation?: VolumeAllocationParams
  excluded_volume_ids?: number[]
}

export interface UpdateCollectionParams {
  status?: number
  storage_policy?: StoragePolicyParams
  disk_type?: string
  capacity_quota_bytes?: number
  ttl_seconds?: number
  description?: string
  volume_allocation?: VolumeAllocationParams
  excluded_volume_ids?: number[]
}

export async function getCollections(): Promise<CollectionInfo[]> {
  const response = await api.get('/collections')
  return response.data.data
}

export async function getCollection(name: string): Promise<CollectionInfo> {
  const response = await api.get(`/collections/${name}`)
  return response.data.data
}

export async function createCollection(params: CreateCollectionParams): Promise<CollectionInfo> {
  const response = await api.post('/collections', params)
  return response.data.data
}

export async function updateCollection(name: string, params: UpdateCollectionParams): Promise<CollectionInfo> {
  const response = await api.put(`/collections/${name}`, params)
  return response.data.data
}

export async function deleteCollection(name: string): Promise<void> {
  await api.delete(`/collections/${name}`)
}

export async function getCollectionStats(name: string): Promise<CollectionStats> {
  const response = await api.get(`/collections/${name}/stats`)
  return response.data.data
}

// ===== Master Raft =====
export async function getMasterStatus(): Promise<MasterStatus> {
  if (useMock) {
    const masters = mockNodes.filter(n => n.node_type === 'master')
    const leader = masters.find(m => m.is_leader) || null
    return {
      nodes: masters,
      leader,
      raft_term: leader?.raft_term ?? 0,
      total_masters: masters.length,
      healthy_masters: masters.filter(m => ['online', 'healthy', 'leader'].includes(m.status)).length,
    }
  }
  const response = await api.get('/master/status')
  return response.data.data
}

export async function transferLeader(targetNodeId: number): Promise<void> {
  if (useMock) return
  await api.post('/master/transfer-leader', { target_node_id: targetNodeId })
}

// ===== Runtime Config (hot-modify) =====
export async function getCircuitBreakerConfig(): Promise<CircuitBreakerConfig> {
  if (useMock) {
    return { failure_threshold: 50, recovery_timeout_ms: 5000, half_open_max_requests: 10 }
  }
  const response = await api.get('/config/circuit-breaker')
  return response.data
}

export async function putCircuitBreakerConfig(cfg: CircuitBreakerConfig): Promise<void> {
  if (useMock) return
  await api.put('/config/circuit-breaker', cfg)
}

export async function getCoalescerConfig(): Promise<CoalescerConfig> {
  if (useMock) {
    return {
      deadline_ms: 2000,
      min_pending_writes: 4,
      max_dirty_bytes_per_entry: 1048576,
      max_dirty_bytes_total: 67108864,
      disabled: false,
    }
  }
  const response = await api.get('/config/coalescer')
  return response.data
}

export async function putCoalescerConfig(cfg: CoalescerConfig): Promise<void> {
  if (useMock) return
  await api.put('/config/coalescer', cfg)
}

// ===== Filer Bucket management (决策 2: 透传到 Monitor /api/filer/buckets*) =====
export interface FilerBucketInfo {
  name: string
  volume_ids: number[]
  size_limit: number
  used_size: number
  creation_time: string
  replication: string
  collection: string
}

export interface FilerBucketListResponse {
  buckets: FilerBucketInfo[]
  total: number
}

export interface CreateFilerBucketParams {
  name: string
  collection?: string
  size_limit?: number
}

export async function listFilerBuckets(): Promise<FilerBucketListResponse> {
  if (useMock) {
    return { buckets: [], total: 0 }
  }
  const response = await api.get('/filer/buckets')
  // 后端 ApiResponse 包装: { success, data, error }
  return response.data.data
}

export async function createFilerBucket(params: CreateFilerBucketParams): Promise<FilerBucketInfo> {
  if (useMock) {
    throw new Error('mock mode does not support creating bucket')
  }
  const response = await api.post('/filer/buckets', params)
  return response.data.data
}

export async function deleteFilerBucket(name: string): Promise<void> {
  if (useMock) return
  await api.delete(`/filer/buckets/${encodeURIComponent(name)}`)
}

export async function setFilerBucketQuota(name: string, sizeLimit: number): Promise<FilerBucketInfo> {
  if (useMock) {
    throw new Error('mock mode does not support setting quota')
  }
  const response = await api.put(`/filer/buckets/${encodeURIComponent(name)}/quota`, {
    size_limit: sizeLimit,
  })
  return response.data.data
}
