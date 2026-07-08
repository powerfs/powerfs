import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import ProtectedRoute from './components/ProtectedRoute'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import Nodes from './pages/Nodes'
import Volumes from './pages/Volumes'
import KV from './pages/KV'
import Alerts from './pages/Alerts'
import S3 from './pages/S3'
import Fuse from './pages/Fuse'
import Users from './pages/Users'
import Roles from './pages/Roles'
import AccessKeys from './pages/AccessKeys'

function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route
        path="/"
        element={
          <ProtectedRoute>
            <Layout />
          </ProtectedRoute>
        }
      >
        <Route
          index
          element={
            <ProtectedRoute requireAdmin>
              <Dashboard />
            </ProtectedRoute>
          }
        />
        <Route
          path="nodes"
          element={
            <ProtectedRoute requireAdmin>
              <Nodes />
            </ProtectedRoute>
          }
        />
        <Route
          path="volumes"
          element={
            <ProtectedRoute requireAdmin>
              <Volumes />
            </ProtectedRoute>
          }
        />
        <Route path="kv" element={<KV />} />
        <Route path="s3" element={<S3 />} />
        <Route
          path="fuse"
          element={
            <ProtectedRoute requireAdmin>
              <Fuse />
            </ProtectedRoute>
          }
        />
        <Route path="alerts" element={<Alerts />} />
        <Route path="access-keys" element={<AccessKeys />} />
        <Route
          path="users"
          element={
            <ProtectedRoute requireAdmin>
              <Users />
            </ProtectedRoute>
          }
        />
        <Route
          path="roles"
          element={
            <ProtectedRoute requireAdmin>
              <Roles />
            </ProtectedRoute>
          }
        />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}

export default App
