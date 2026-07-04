import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Nodes from './pages/Nodes'
import Volumes from './pages/Volumes'
import KV from './pages/KV'
import Alerts from './pages/Alerts'

function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="nodes" element={<Nodes />} />
        <Route path="volumes" element={<Volumes />} />
        <Route path="kv" element={<KV />} />
        <Route path="alerts" element={<Alerts />} />
      </Route>
    </Routes>
  )
}

export default App