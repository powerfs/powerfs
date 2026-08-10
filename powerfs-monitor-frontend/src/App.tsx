import { lazy, Suspense } from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import { Spin } from 'antd'
import Layout from './components/Layout'
import ProtectedRoute from './components/ProtectedRoute'
import Login from './pages/Login'

// Route-level code splitting — each page is loaded on demand,
// reducing the initial bundle from ~2.8MB to just the framework + Layout.
const Dashboard = lazy(() => import('./pages/Dashboard'))
const Nodes = lazy(() => import('./pages/Nodes'))
const Volumes = lazy(() => import('./pages/Volumes'))
const Collections = lazy(() => import('./pages/Collections'))
const StorageDevices = lazy(() => import('./pages/StorageDevices'))
const BitrotScrub = lazy(() => import('./pages/BitrotScrub'))
const KV = lazy(() => import('./pages/KV'))
const Alerts = lazy(() => import('./pages/Alerts'))
const S3 = lazy(() => import('./pages/S3'))
const Fuse = lazy(() => import('./pages/Fuse'))
const Filer = lazy(() => import('./pages/Filer'))
const Shards = lazy(() => import('./pages/Shards'))
const ShardBalancing = lazy(() => import('./pages/ShardBalancing'))
const Conflicts = lazy(() => import('./pages/Conflicts'))
const Users = lazy(() => import('./pages/Users'))
const Roles = lazy(() => import('./pages/Roles'))
const AccessKeys = lazy(() => import('./pages/AccessKeys'))
const Benchmark = lazy(() => import('./pages/Benchmark'))
const ClusterTopology = lazy(() => import('./pages/ClusterTopology'))
const CapacityPlanning = lazy(() => import('./pages/CapacityPlanning'))
const MasterRaft = lazy(() => import('./pages/MasterRaft'))
const RuntimeConfig = lazy(() => import('./pages/RuntimeConfig'))

function PageLoading() {
  return (
    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 300 }}>
      <Spin size="large" />
    </div>
  )
}

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
              <Suspense fallback={<PageLoading />}>
                <Dashboard />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="nodes"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Nodes />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="cluster-topology"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <ClusterTopology />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="master-raft"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <MasterRaft />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="capacity-planning"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <CapacityPlanning />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="storage-devices"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <StorageDevices />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="volumes"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Volumes />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="collections"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Collections />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="bitrot-scrub"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <BitrotScrub />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="kv"
          element={
            <Suspense fallback={<PageLoading />}>
              <KV />
            </Suspense>
          }
        />
        <Route
          path="benchmark"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Benchmark />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="s3"
          element={
            <Suspense fallback={<PageLoading />}>
              <S3 />
            </Suspense>
          }
        />
        <Route
          path="fuse"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Fuse />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="conflicts"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Conflicts />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="filer"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Filer />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="shards"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Shards />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="shard-balancing"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <ShardBalancing />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="alerts"
          element={
            <Suspense fallback={<PageLoading />}>
              <Alerts />
            </Suspense>
          }
        />
        <Route
          path="runtime-config"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <RuntimeConfig />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="access-keys"
          element={
            <Suspense fallback={<PageLoading />}>
              <AccessKeys />
            </Suspense>
          }
        />
        <Route
          path="users"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Users />
              </Suspense>
            </ProtectedRoute>
          }
        />
        <Route
          path="roles"
          element={
            <ProtectedRoute requireAdmin>
              <Suspense fallback={<PageLoading />}>
                <Roles />
              </Suspense>
            </ProtectedRoute>
          }
        />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}

export default App
