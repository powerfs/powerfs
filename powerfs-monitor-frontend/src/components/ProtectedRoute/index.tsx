import { useEffect, useState, type ReactNode } from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { subscribe, isAuthenticated, getCurrentUser } from '@/services/auth'

interface ProtectedRouteProps {
  children: ReactNode
  requireAdmin?: boolean
}

// 路由守卫：未登录则跳转 /login，非 admin 访问 admin 路由则跳转 /
export default function ProtectedRoute({ children, requireAdmin = false }: ProtectedRouteProps) {
  const [authed, setAuthed] = useState(isAuthenticated())
  const location = useLocation()

  useEffect(() => {
    const unsubscribe = subscribe(() => {
      setAuthed(isAuthenticated())
    })
    return unsubscribe
  }, [])

  if (!authed) {
    return <Navigate to="/login" state={{ from: location.pathname }} replace />
  }

  if (requireAdmin && getCurrentUser()?.role !== 'admin') {
    return <Navigate to="/kv" replace />
  }

  return <>{children}</>
}
