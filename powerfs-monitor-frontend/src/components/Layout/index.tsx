import { useEffect, useRef, useState } from 'react'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import {
  Layout,
  Menu,
  Button,
  Space,
  Dropdown,
  Avatar,
  Typography,
  App,
  Tooltip,
  Tag,
} from 'antd'
import type { MenuProps } from 'antd'
import {
  DashboardOutlined,
  DatabaseOutlined,
  KeyOutlined,
  BellOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  CloudOutlined,
  FolderOpenOutlined,
  SearchOutlined,
  UserOutlined,
  LogoutOutlined,
  TeamOutlined,
  SafetyCertificateOutlined,
  LockOutlined,
  WarningOutlined,
  SafetyOutlined,
  BulbOutlined,
  BulbFilled,
  DesktopOutlined,
  SettingOutlined,
  // AppstoreOutlined,  // used by StorageDevices entry; hidden pending backend supplement
  RocketOutlined,
  CloudServerOutlined,
  ClusterOutlined,
  LineChartOutlined,
  TranslationOutlined,
  ApiOutlined,
} from '@ant-design/icons'
import {
  subscribe,
  getCurrentUser,
  logout as authLogout,
  type CurrentUser,
} from '@/services/auth'
import { useTheme, type ThemeMode } from '@/styles/ThemeContext'
import GlobalSearch, { type GlobalSearchHandle } from '@/components/GlobalSearch'
import Logo from '@/components/Logo'
import { LANGUAGES, type LangCode } from '@/i18n'
import { useMetricStream } from '@/hooks/useMetricStream'

const { Header, Sider, Content } = Layout
const { Text } = Typography

type MenuItem = Required<MenuProps>['items'][number]

function AppLayout() {
  const { t, i18n } = useTranslation(['common', 'nav'])
  const [collapsed, setCollapsed] = useState(false)
  const location = useLocation()
  const navigate = useNavigate()
  const [user, setUser] = useState<CurrentUser | null>(getCurrentUser())
  const { mode, setMode } = useTheme()
  const searchRef = useRef<GlobalSearchHandle>(null)
  const { message } = App.useApp()

  useEffect(() => {
    const unsubscribe = subscribe(() => {
      setUser(getCurrentUser())
    })
    return unsubscribe
  }, [])

  const isAdmin = user?.role === 'admin'

  // Global WebSocket status indicator (no source filter — just track
  // connection health for the header badge).
  const { status: wsStatus } = useMetricStream()

  // Group menu items into logical categories
  const menuItems: MenuItem[] = [
    // ── Overview ──
    {
      key: 'grp-overview',
      type: 'group',
      label: t('nav:groups.overview'),
      children: [
        { key: '/', icon: <DashboardOutlined />, label: t('nav:items.dashboard') },
        { key: '/cluster-topology', icon: <ClusterOutlined />, label: t('nav:items.clusterTopology') },
        { key: '/master-raft', icon: <CloudServerOutlined />, label: t('nav:items.masterRaft') },
      ],
    },
    // ── Infrastructure ──
    ...(isAdmin
      ? [{
          key: 'grp-infra',
          type: 'group' as const,
          label: t('nav:groups.infrastructure'),
          children: [
            { key: '/capacity-planning', icon: <LineChartOutlined />, label: t('nav:items.capacityPlanning') },
            // TODO: restore StorageDevices entry after backend supplement (decision 1)
            // { key: '/storage-devices', icon: <AppstoreOutlined />, label: t('nav:items.storageDevices') },
          ],
        }]
      : []),
    // ── Storage ──
    ...(isAdmin
      ? [{
          key: 'grp-storage',
          type: 'group' as const,
          label: t('nav:groups.storage'),
          children: [
            { key: '/volumes', icon: <DatabaseOutlined />, label: t('nav:items.volumes') },
            { key: '/collections', icon: <DatabaseOutlined />, label: t('nav:items.collections') },
            { key: '/bitrot-scrub', icon: <SafetyOutlined />, label: t('nav:items.bitrotScrub') },
          ],
        }]
      : []),
    // ── Metadata ──
    ...(isAdmin
      ? [{
          key: 'grp-meta',
          type: 'group' as const,
          label: t('nav:groups.metadata'),
          children: [
            {
              key: 'filer-submenu',
              icon: <CloudServerOutlined />,
              label: t('nav:items.filerManagement'),
              children: [
                { key: '/filer', label: t('nav:items.filerOverview') },
                { key: '/conflicts', icon: <WarningOutlined />, label: t('nav:items.conflicts') },
              ],
            },
            { key: '/shards', icon: <ClusterOutlined />, label: t('nav:items.shards') },
            { key: '/shard-balancing', icon: <DatabaseOutlined />, label: t('nav:items.shardBalancing') },
          ],
        }]
      : []),
    // ── Clients & Performance ──
    {
      key: 'grp-clients',
      type: 'group',
      label: t('nav:groups.clientsAndPerformance'),
      children: [
        ...(isAdmin
          ? [
              { key: '/fuse', icon: <FolderOpenOutlined />, label: t('nav:items.fsManagement') },
              { key: '/benchmark', icon: <RocketOutlined />, label: t('nav:items.benchmark') },
            ]
          : []),
        { key: '/s3', icon: <CloudOutlined />, label: t('nav:items.s3') },
        { key: '/kv', icon: <KeyOutlined />, label: t('nav:items.kv') },
      ],
    },
    // ── Operations ──
    {
      key: 'grp-operations',
      type: 'group',
      label: t('nav:groups.operations'),
      children: [
        { key: '/alerts', icon: <BellOutlined />, label: t('nav:items.alerts') },
        ...(isAdmin
          ? [
              { key: '/runtime-config', icon: <SettingOutlined />, label: t('nav:items.runtimeConfig') },
            ]
          : []),
      ],
    },
    // ── Security ──
    {
      key: 'grp-security',
      type: 'group',
      label: t('nav:groups.security'),
      children: [
        { key: '/access-keys', icon: <LockOutlined />, label: t('nav:items.myAccessKeys') },
        ...(isAdmin
          ? [
              { key: '/users', icon: <TeamOutlined />, label: t('nav:items.users') },
              { key: '/roles', icon: <SafetyCertificateOutlined />, label: t('nav:items.roles') },
            ]
          : []),
      ],
    },
  ]

  const handleLogout = () => {
    authLogout()
    message.success(t('common:header.loggedOut'))
    navigate('/login', { replace: true })
  }

  const userMenuItems: MenuProps['items'] = [
    {
      key: 'user-info',
      label: (
        <div style={{ padding: '4px 8px' }}>
          <div style={{ fontWeight: 500 }}>{user?.username ?? '-'}</div>
          <Text type="secondary" style={{ fontSize: 12 }}>
            {user?.role === 'admin' ? t('common:header.roleAdmin') : t('common:header.roleUser')}
          </Text>
        </div>
      ),
      disabled: true,
    },
    { type: 'divider' },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: t('common:header.logout'),
      onClick: handleLogout,
    },
  ]

  const themeMenuItems: MenuProps['items'] = [
    {
      key: 'light',
      icon: <BulbOutlined />,
      label: t('common:theme.light'),
      onClick: () => setMode('light' as ThemeMode),
    },
    {
      key: 'dark',
      icon: <BulbFilled />,
      label: t('common:theme.dark'),
      onClick: () => setMode('dark' as ThemeMode),
    },
    {
      key: 'auto',
      icon: <DesktopOutlined />,
      label: t('common:theme.auto'),
      onClick: () => setMode('auto' as ThemeMode),
    },
  ]

  const themeLabel = mode === 'light'
    ? t('common:theme.light')
    : mode === 'dark'
      ? t('common:theme.dark')
      : t('common:theme.auto')

  const currentLang = (LANGUAGES.find(l => l.code === i18n.language as LangCode) ?? LANGUAGES[0])

  const languageMenuItems: MenuProps['items'] = LANGUAGES.map(l => ({
    key: l.code,
    label: (
      <Space size={8}>
        <span>{l.flag}</span>
        <span>{l.label}</span>
        {l.code === currentLang.code && <Tag color="blue" style={{ marginLeft: 'auto' }}>✓</Tag>}
      </Space>
    ),
    onClick: () => void i18n.changeLanguage(l.code),
  }))

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        width={240}
        style={{
          background: 'var(--pf-sider-bg)',
          borderRight: '1px solid var(--pf-sider-border)',
          position: 'sticky',
          top: 0,
          height: '100vh',
        }}
      >
        <div
          style={{
            padding: '16px',
            textAlign: 'center',
            borderBottom: '1px solid var(--pf-sider-border)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: collapsed ? 0 : 10,
          }}
        >
          <Logo size={collapsed ? 28 : 32} style={{ flexShrink: 0 }} />
          {!collapsed && (
            <span
              className="pf-gradient-text"
              style={{ fontSize: 18, fontWeight: 700, letterSpacing: 0.5 }}
            >
              PowerFS
            </span>
          )}
        </div>
        <Menu
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{
            background: 'transparent',
            borderRight: 'none',
            paddingTop: 8,
          }}
        />
      </Sider>

      <Layout>
        <Header
          style={{
            background: 'var(--pf-color-bg-container)',
            padding: '0 24px',
            borderBottom: '1px solid var(--pf-color-border)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            position: 'sticky',
            top: 0,
            zIndex: 10,
          }}
        >
          <Space size={16}>
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed(!collapsed)}
            />
            <Text strong style={{ fontSize: 16 }}>{t('common:header.title')}</Text>
          </Space>

          <Space size={16}>
            {/* Cluster health badge */}
            <Tooltip title={t('common:header.clusterHealth')}>
              <Tag
                color="success"
                style={{
                  margin: 0,
                  padding: '2px 12px',
                  borderRadius: 12,
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                }}
              >
                <span
                  className="pf-pulse"
                  style={{
                    width: 6,
                    height: 6,
                    borderRadius: '50%',
                    background: 'var(--pf-color-success)',
                    display: 'inline-block',
                  }}
                />
                {t('common:header.healthy')}
              </Tag>
            </Tooltip>

            {/* WebSocket connection status badge */}
            <Tooltip
              title={
                wsStatus === 'open'
                  ? t('common:realtimeStream') + ' · ' + t('common:connected')
                  : wsStatus === 'connecting'
                    ? t('common:reconnecting')
                    : t('common:disconnected')
              }
            >
              <Tag
                color={wsStatus === 'open' ? 'processing' : wsStatus === 'connecting' ? 'warning' : 'error'}
                style={{
                  margin: 0,
                  padding: '2px 10px',
                  borderRadius: 12,
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                }}
              >
                <ApiOutlined style={{ fontSize: 11 }} />
                {wsStatus === 'open'
                  ? t('common:connected')
                  : wsStatus === 'connecting'
                    ? t('common:reconnecting')
                    : t('common:disconnected')}
              </Tag>
            </Tooltip>

            {/* Global search trigger */}
            <Tooltip title={t('common:header.globalSearch')}>
              <Button
                type="text"
                icon={<SearchOutlined />}
                onClick={() => searchRef.current?.open()}
              />
            </Tooltip>

            {/* Language switcher */}
            <Dropdown menu={{ items: languageMenuItems }} placement="bottomRight">
              <Tooltip title={t('common:language.switchTo')}>
                <Button type="text">
                  <Space size={4}>
                    <TranslationOutlined />
                    <span>{currentLang.flag}</span>
                    {currentLang.label}
                  </Space>
                </Button>
              </Tooltip>
            </Dropdown>

            {/* Theme switcher */}
            <Dropdown menu={{ items: themeMenuItems }} placement="bottomRight">
              <Tooltip title={t('common:header.theme', { theme: themeLabel })}>
                <Button type="text">
                  <Space size={4}>
                    {mode === 'dark' ? <BulbFilled /> : <BulbOutlined />}
                    {themeLabel}
                  </Space>
                </Button>
              </Tooltip>
            </Dropdown>

            {/* User menu */}
            <Dropdown menu={{ items: userMenuItems }} placement="bottomRight">
              <Space style={{ cursor: 'pointer', padding: '0 8px' }}>
                <Avatar size="small" icon={<UserOutlined />} />
                <span>{user?.username ?? t('common:header.notLoggedIn')}</span>
              </Space>
            </Dropdown>
          </Space>
        </Header>

        <Content
          style={{
            margin: '24px 16px',
            padding: 24,
            minHeight: 280,
            background: 'var(--pf-color-bg)',
            borderRadius: 12,
          }}
        >
          <Outlet />
        </Content>
      </Layout>

      {/* Global command palette (Cmd+K / Ctrl+K) */}
      <GlobalSearch ref={searchRef} isAdmin={isAdmin} />
    </Layout>
  )
}

export default AppLayout
