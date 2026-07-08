import axios from 'axios'

// 认证相关类型定义
export type UserRole = 'admin' | 'user'
export type UserStatus = 'active' | 'inactive' | 'locked'

export interface CurrentUser {
  user_id: string
  username: string
  role: UserRole
  status: UserStatus
}

export interface LoginRequest {
  username: string
  password: string
}

export interface TokenPair {
  token: string
  refresh_token: string
  expires_in: number
  user: CurrentUser
}

const STORAGE_KEY = 'powerfs_auth'

// 从 localStorage 读取已保存的认证信息
function loadStoredAuth(): TokenPair | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return null
    return JSON.parse(raw) as TokenPair
  } catch {
    return null
  }
}

// 保存认证信息到 localStorage
function saveAuth(auth: TokenPair | null) {
  if (auth) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(auth))
  } else {
    localStorage.removeItem(STORAGE_KEY)
  }
}

let currentAuth: TokenPair | null = loadStoredAuth()

// 订阅者模式，便于 UI 同步登录状态变化
type Listener = (auth: TokenPair | null) => void
const listeners = new Set<Listener>()

function notify() {
  listeners.forEach((fn) => fn(currentAuth))
}

export function subscribe(listener: Listener): () => void {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

export function getCurrentAuth(): TokenPair | null {
  return currentAuth
}

export function getCurrentUser(): CurrentUser | null {
  return currentAuth?.user ?? null
}

export function getToken(): string | null {
  return currentAuth?.token ?? null
}

export function isAuthenticated(): boolean {
  return currentAuth !== null
}

export function isAdmin(): boolean {
  return currentAuth?.user?.role === 'admin'
}

// 登录
export async function login(username: string, password: string): Promise<TokenPair> {
  const response = await axios.post<{ code: number; message: string; data: TokenPair }>(
    '/api/auth/login',
    { username, password },
  )
  if (response.data.code !== 200) {
    throw new Error(response.data.message || '登录失败')
  }
  const pair = response.data.data
  if (!pair) {
    throw new Error('登录失败')
  }
  currentAuth = pair
  saveAuth(pair)
  notify()
  return pair
}

// 登出
export function logout() {
  currentAuth = null
  saveAuth(null)
  notify()
}

// 刷新 access token
export async function refreshAccessToken(): Promise<string | null> {
  if (!currentAuth?.refresh_token) {
    logout()
    return null
  }
  try {
    const response = await axios.post<{ code: number; message: string; data: TokenPair }>(
      '/api/auth/refresh',
      { refresh_token: currentAuth.refresh_token },
    )
    const pair = response.data.data
    currentAuth = pair
    saveAuth(pair)
    notify()
    return pair.token
  } catch {
    logout()
    return null
  }
}

// 供 axios 拦截器使用：判断 URL 是否为公开路由
export function isPublicUrl(url: string | undefined): boolean {
  if (!url) return false
  return url.includes('/api/auth/login') || url.includes('/api/auth/refresh')
}
