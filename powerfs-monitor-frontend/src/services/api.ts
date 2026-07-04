import axios from 'axios'
import type { NodeInfo, VolumeInfo, KVSessionInfo, AlertInfo, AlertRule, ClusterMetrics, KVMetrics, TimeSeriesData } from '@/types'
import { mockNodes, mockVolumes, mockKVSessions, mockAlerts, mockAlertRules, mockClusterMetrics, mockKVMetrics, generateTimeSeriesData } from '@/utils/mockData'

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
  return response.data
}

export async function getKVMetrics(): Promise<KVMetrics> {
  if (useMock) {
    return mockKVMetrics
  }
  const response = await api.get('/metrics/kv')
  return response.data
}

export async function getNodes(): Promise<NodeInfo[]> {
  if (useMock) {
    return mockNodes
  }
  const response = await api.get('/nodes')
  return response.data
}

export async function getNode(id: string): Promise<NodeInfo> {
  if (useMock) {
    return mockNodes.find(n => n.id === id) || mockNodes[0]
  }
  const response = await api.get(`/nodes/${id}`)
  return response.data
}

export async function getVolumes(): Promise<VolumeInfo[]> {
  if (useMock) {
    return mockVolumes
  }
  const response = await api.get('/volumes')
  return response.data
}

export async function getVolume(id: number): Promise<VolumeInfo> {
  if (useMock) {
    return mockVolumes.find(v => v.id === id) || mockVolumes[0]
  }
  const response = await api.get(`/volumes/${id}`)
  return response.data
}

export async function getKVSessions(): Promise<KVSessionInfo[]> {
  if (useMock) {
    return mockKVSessions
  }
  const response = await api.get('/kv/sessions')
  return response.data
}

export async function getKVSession(id: string): Promise<KVSessionInfo> {
  if (useMock) {
    return mockKVSessions.find(s => s.id === id) || mockKVSessions[0]
  }
  const response = await api.get(`/kv/sessions/${id}`)
  return response.data
}

export async function getAlerts(): Promise<AlertInfo[]> {
  if (useMock) {
    return mockAlerts
  }
  const response = await api.get('/alerts')
  return response.data
}

export async function getAlertRules(): Promise<AlertRule[]> {
  if (useMock) {
    return mockAlertRules
  }
  const response = await api.get('/alert-rules')
  return response.data
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
  await api.delete(`/kv/sessions/${id}`)
}

export async function deleteNode(id: string): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/nodes/${id}`)
}

export async function deleteVolume(id: number): Promise<void> {
  if (useMock) {
    return
  }
  await api.delete(`/volumes/${id}`)
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
  return response.data
}