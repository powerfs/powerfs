import api from './api'
import type { UserRole, UserStatus } from './auth'

// 用户信息（来自后端 /api/users）
export interface User {
  id: string
  username: string
  role: UserRole
  status: UserStatus
  email?: string | null
  created_at: string
  updated_at: string
  last_login_at?: string | null
}

export interface CreateUserRequest {
  username: string
  password: string
  role?: UserRole
  email?: string
}

export interface UpdateUserRequest {
  email?: string
  role?: UserRole
  status?: UserStatus
  password?: string
}

export async function listUsers(): Promise<User[]> {
  const response = await api.get('/users')
  return response.data.data
}

export async function getUser(id: string): Promise<User> {
  const response = await api.get(`/users/${id}`)
  return response.data.data
}

export async function createUser(req: CreateUserRequest): Promise<User> {
  const response = await api.post('/users', req)
  return response.data.data
}

export async function updateUser(id: string, req: UpdateUserRequest): Promise<User> {
  const response = await api.put(`/users/${id}`, req)
  return response.data.data
}

export async function deleteUser(id: string): Promise<void> {
  await api.delete(`/users/${id}`)
}

export async function getCurrentUserInfo(): Promise<User> {
  const response = await api.get('/auth/me')
  return response.data.data
}
