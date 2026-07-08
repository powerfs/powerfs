import api from './api'

// 角色信息
export interface Role {
  id: string
  name: string
  description: string
  permissions: string[]
  created_at: string
  updated_at: string
}

export interface CreateRoleRequest {
  name: string
  description?: string
  permissions: string[]
}

export interface UpdateRoleRequest {
  name?: string
  description?: string
  permissions?: string[]
}

export async function listRoles(): Promise<Role[]> {
  const response = await api.get('/roles')
  return response.data.data
}

export async function getRole(id: string): Promise<Role> {
  const response = await api.get(`/roles/${id}`)
  return response.data.data
}

export async function createRole(req: CreateRoleRequest): Promise<Role> {
  const response = await api.post('/roles', req)
  return response.data.data
}

export async function updateRole(id: string, req: UpdateRoleRequest): Promise<Role> {
  const response = await api.put(`/roles/${id}`, req)
  return response.data.data
}

export async function deleteRole(id: string): Promise<void> {
  await api.delete(`/roles/${id}`)
}
