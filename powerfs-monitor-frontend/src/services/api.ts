import axios from 'axios'
import type { NodeInfo, VolumeInfo, KVSessionInfo, AlertInfo, AlertRule, ClusterMetrics, KVMetrics, TimeSeriesData, BucketInfo, ObjectInfo, MultipartUploadInfo, S3Metrics, FuseMount, S3AccessKey } from '@/types'
import { mockNodes, mockVolumes, mockKVSessions, mockAlerts, mockAlertRules, mockClusterMetrics, mockKVMetrics, generateTimeSeriesData, mockBuckets, mockObjects, mockMultipartUploads, mockS3Metrics } from '@/utils/mockData'

const api = axios.create({
  baseURL: '/api',
  timeout: 10000,
})

let useMock = false

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