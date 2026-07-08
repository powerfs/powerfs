import api from './api'

// S3 AccessKey 信息（不含 secret_key_hash）
export interface S3AccessKeyInfo {
  id: string
  user_id: string
  access_key: string
  status: 'active' | 'inactive'
  created_at: string
  last_used_at?: string | null
}

// 创建后返回的信息（包含一次性明文 secret_key）
export interface CreatedAccessKey extends S3AccessKeyInfo {
  secret_key: string
}

export async function listAccessKeys(): Promise<S3AccessKeyInfo[]> {
  const response = await api.get('/s3/keys')
  return response.data.data
}

export async function createAccessKey(): Promise<CreatedAccessKey> {
  const response = await api.post('/s3/keys')
  return response.data.data
}

export async function deleteAccessKey(id: string): Promise<void> {
  await api.delete(`/s3/keys/${id}`)
}
