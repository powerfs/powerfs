import axios from 'axios'
import type { NodeInfo, VolumeInfo, KVSessionInfo, AlertInfo, AlertRule, ClusterMetrics, KVMetrics, TimeSeriesData, BucketInfo, ObjectInfo, MultipartUploadInfo, S3Metrics, FuseMount, S3AccessKey, KVNamespace, KVAccessKey, ConflictRecord, ConflictStats, AutoResolveResult, BatchResolveResult, BatchIgnoreResult } from '@/types'
import { mockNodes, mockVolumes, mockKVSessions, mockAlerts, mockAlertRules, mockClusterMetrics, mockKVMetrics, generateTimeSeriesData, mockBuckets, mockObjects, mockMultipartUploads, mockS3Metrics } from '@/utils/mockData'
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

let useMock = false

let mockKVNamespaces: KVNamespace[] = [
  { id: 'ns-1', name: 'default', owner_id: 'user-1', created_at: Date.now() - 86400000, updated_at: Date.now() - 86400000 },
  { id: 'ns-2', name: 'production', owner_id: 'user-1', created_at: Date.now() - 172800000, updated_at: Date.now() - 86400000 },
]

export function setUseMock(value: boolean) {
  useMock = value
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

export async function getMetricHistory(metric: string): Promise<TimeSeriesData[]> {
  if (useMock) {
    const baseValues: Record<string, number> = {
      'powerfs_node_disk_usage': 65,
      'powerfs_node_cpu_usage': 45,
      'powerfs_kv_hit_ratio': 90,
      'powerfs_kv_memory_used': 50,
    }
    return generateTimeSeriesData(24, baseValues[metric] || 100, 20)
  }
  const response = await api.get(`/metrics/history/${metric}`)
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
    return []
  }
  const response = await api.get('/fuse/mounts')
  return response.data.data
}

export async function createFuseMount(mount: {
  mount_point: string
  collection: string
  replication: string
  master: string
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