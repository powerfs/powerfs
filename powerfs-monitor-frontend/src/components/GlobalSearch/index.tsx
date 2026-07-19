/**
 * GlobalSearch — Cmd+K / Ctrl+K command palette.
 *
 * Listens for the keyboard shortcut, opens a modal with a searchable list of
 * navigation targets. Filters pages by title; Enter navigates to the first
 * match, click navigates to the chosen item.
 */

import {
  forwardRef,
  useEffect,
  useMemo,
  useState,
  useCallback,
  useImperativeHandle,
} from 'react'
import { Modal, Input, List, Typography, Tag } from 'antd'
import {
  SearchOutlined,
  DashboardOutlined,
  BellOutlined,
  HddOutlined,
  AppstoreOutlined,
  FolderOpenOutlined,
  DatabaseOutlined,
  SafetyOutlined,
  CloudOutlined,
  WarningOutlined,
  KeyOutlined,
  LockOutlined,
  TeamOutlined,
  SafetyCertificateOutlined,
} from '@ant-design/icons'
import { useNavigate } from 'react-router-dom'
import React from 'react'

const { Text } = Typography

interface SearchEntry {
  key: string
  title: string
  path: string
  icon: React.ReactNode
  group: string
  adminOnly?: boolean
}

const ALL_ENTRIES: SearchEntry[] = [
  { key: 'dashboard', title: '仪表盘', path: '/', icon: <DashboardOutlined />, group: '总览' },
  { key: 'alerts', title: '告警中心', path: '/alerts', icon: <BellOutlined />, group: '总览' },
  { key: 'nodes', title: '节点管理', path: '/nodes', icon: <HddOutlined />, group: '基础设施', adminOnly: true },
  { key: 'storage-devices', title: '存储设备', path: '/storage-devices', icon: <AppstoreOutlined />, group: '基础设施', adminOnly: true },
  { key: 'fuse', title: 'FUSE 管理', path: '/fuse', icon: <FolderOpenOutlined />, group: '基础设施', adminOnly: true },
  { key: 'volumes', title: 'Volume 管理', path: '/volumes', icon: <DatabaseOutlined />, group: '存储', adminOnly: true },
  { key: 'bitrot-scrub', title: 'Bitrot 扫描', path: '/bitrot-scrub', icon: <SafetyOutlined />, group: '存储', adminOnly: true },
  { key: 's3', title: 'S3 管理', path: '/s3', icon: <CloudOutlined />, group: '存储' },
  { key: 'conflicts', title: '冲突管理', path: '/conflicts', icon: <WarningOutlined />, group: '元数据', adminOnly: true },
  { key: 'kv', title: 'KV 管理', path: '/kv', icon: <KeyOutlined />, group: '性能' },
  { key: 'access-keys', title: '我的密钥', path: '/access-keys', icon: <LockOutlined />, group: '安全' },
  { key: 'users', title: '用户管理', path: '/users', icon: <TeamOutlined />, group: '安全', adminOnly: true },
  { key: 'roles', title: '角色管理', path: '/roles', icon: <SafetyCertificateOutlined />, group: '安全', adminOnly: true },
]

export interface GlobalSearchProps {
  /** Filter entries by admin role */
  isAdmin?: boolean
}

export interface GlobalSearchHandle {
  open: () => void
  close: () => void
  toggle: () => void
}

const GlobalSearch = forwardRef<GlobalSearchHandle, GlobalSearchProps>(
  ({ isAdmin = false }, ref) => {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const navigate = useNavigate()

  useImperativeHandle(ref, () => ({
    open: () => setOpen(true),
    close: () => setOpen(false),
    toggle: () => setOpen(o => !o),
  }), [])

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault()
      setOpen(o => !o)
    } else if (e.key === 'Escape') {
      setOpen(false)
    }
  }, [])

  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [handleKeyDown])

  const filtered = useMemo(() => {
    const pool = isAdmin ? ALL_ENTRIES : ALL_ENTRIES.filter(e => !e.adminOnly)
    if (!query.trim()) return pool
    const q = query.toLowerCase()
    return pool.filter(
      e => e.title.toLowerCase().includes(q) || e.group.toLowerCase().includes(q),
    )
  }, [query, isAdmin])

  const handleSelect = (path: string) => {
    navigate(path)
    setOpen(false)
    setQuery('')
  }

  return (
    <Modal
      open={open}
      onCancel={() => setOpen(false)}
      footer={null}
      width={560}
      centered
      styles={{ body: { padding: 0 } }}
      title={
        <Input
          autoFocus
          prefix={<SearchOutlined style={{ color: 'var(--pf-color-text-tertiary)' }} />}
          placeholder="搜索页面、节点、卷…（按 Enter 跳转，Esc 关闭）"
          value={query}
          onChange={e => setQuery(e.target.value)}
          onPressEnter={() => filtered[0] && handleSelect(filtered[0].path)}
          variant="borderless"
          size="large"
        />
      }
    >
      <List
        dataSource={filtered}
        style={{ maxHeight: 400, overflowY: 'auto' }}
        renderItem={(item, idx) => (
          <List.Item
            key={item.key}
            onClick={() => handleSelect(item.path)}
            style={{
              padding: '12px 16px',
              cursor: 'pointer',
              background:
                idx === 0 && query ? 'var(--pf-color-bg-hover)' : 'transparent',
              borderBottom: '1px solid var(--pf-color-border-secondary)',
              display: 'flex',
              alignItems: 'center',
              gap: 12,
            }}
          >
            <span
              style={{
                width: 32,
                height: 32,
                borderRadius: 8,
                background: 'var(--pf-gradient-brand-soft)',
                color: 'var(--pf-color-brand)',
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: 16,
              }}
            >
              {item.icon}
            </span>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 14, fontWeight: 500 }}>{item.title}</div>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {item.path}
              </Text>
            </div>
            <Tag>{item.group}</Tag>
          </List.Item>
        )}
        locale={{ emptyText: '没有匹配项' }}
      />
    </Modal>
  )
  },
)

GlobalSearch.displayName = 'GlobalSearch'

export default GlobalSearch
