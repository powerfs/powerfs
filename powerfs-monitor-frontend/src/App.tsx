import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import ProtectedRoute from './components/ProtectedRoute'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import Nodes from './pages/Nodes'
import Volumes from './pages/Volumes'
import StorageDevices from './pages/StorageDevices'
import BitrotScrub from './pages/BitrotScrub'
import KV from './pages/KV'
import Alerts from './pages/Alerts'
import S3 from './pages/S3'
import Fuse from './pages/Fuse'
import Filer from './pages/Filer'
import Shards from './pages/Shards'
import ShardBalancing from './pages/ShardBalancing'
import Conflicts from './pages/Conflicts'
import Users from './pages/Users'
import Roles from './pages/Roles'
import AccessKeys from './pages/AccessKeys'
import Benchmark from './pages/Benchmark'

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
          path="storage-devices"
          element={
            <ProtectedRoute requireAdmin>
              <StorageDevices />
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
        <Route
          path="bitrot-scrub"
          element={
            <ProtectedRoute requireAdmin>
              <BitrotScrub />
            </ProtectedRoute>
          }
        />
        <Route path="kv" element={<KV />} />
        <Route
          path="benchmark"
          element={
            <ProtectedRoute requireAdmin>
              <Benchmark />
            </ProtectedRoute>
          }
        />
        <Route path="s3" element={<S3 />} />
        <Route
          path="fuse"
          element={
            <ProtectedRoute requireAdmin>
              <Fuse />
            </ProtectedRoute>
          }
        />
        <Route
          path="conflicts"
          element={
            <ProtectedRoute requireAdmin>
              <Conflicts />
            </ProtectedRoute>
          }
        />
        <Route
          path="filer"
          element={
            <ProtectedRoute requireAdmin>
              <Filer />
            </ProtectedRoute>
          }
        />
        <Route
          path="shards"
          element={
            <ProtectedRoute requireAdmin>
              <Shards />
            </ProtectedRoute>
          }
        />
        <Route
          path="shard-balancing"
          element={
            <ProtectedRoute requireAdmin>
              <ShardBalancing />
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
