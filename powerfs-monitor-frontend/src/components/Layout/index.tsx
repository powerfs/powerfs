import { useEffect, useRef, useState } from 'react'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
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
  RocketOutlined,
  CloudOutlined,
  FolderOpenOutlined,
  SearchOutlined,
  UserOutlined,
  LogoutOutlined,
  TeamOutlined,
  SafetyCertificateOutlined,
  LockOutlined,
  WarningOutlined,
  HddOutlined,
  SafetyOutlined,
  BulbOutlined,
  BulbFilled,
  DesktopOutlined,
  AppstoreOutlined,
} from '@ant-design/icons'
import {
  subscribe,
  getCurrentUser,
  logout as authLogout,
  type CurrentUser,
} from '@/services/auth'
import { useTheme, type ThemeMode } from '@/styles/ThemeContext'
import GlobalSearch, { type GlobalSearchHandle } from '@/components/GlobalSearch'

const { Header, Sider, Content } = Layout
const { Text } = Typography

type MenuItem = Required<MenuProps>['items'][number]

function AppLayout() {
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

  // Group menu items into 5 categories
  const menuItems: MenuItem[] = [
    // ── 总览 ──
    {
      key: 'grp-overview',
      type: 'group',
      label: '总览',
      children: [
        { key: '/', icon: <DashboardOutlined />, label: '仪表盘' },
        { key: '/alerts', icon: <BellOutlined />, label: '告警中心' },
      ],
    },
    // ── 基础设施 ──
    ...(isAdmin
      ? [{
          key: 'grp-infra',
          type: 'group' as const,
          label: '基础设施',
          children: [
            { key: '/nodes', icon: <HddOutlined />, label: '节点管理' },
            { key: '/storage-devices', icon: <AppstoreOutlined />, label: '存储设备' },
            { key: '/fuse', icon: <FolderOpenOutlined />, label: 'FUSE 管理' },
          ],
        }]
      : []),
    // ── 存储 ──
    {
      key: 'grp-storage',
      type: 'group',
      label: '存储',
      children: [
        ...(isAdmin
          ? [
              { key: '/volumes', icon: <DatabaseOutlined />, label: 'Volume 管理' },
              { key: '/bitrot-scrub', icon: <SafetyOutlined />, label: 'Bitrot 扫描' },
            ]
          : []),
        { key: '/s3', icon: <CloudOutlined />, label: 'S3 管理' },
      ],
    },
    // ── 元数据 ──
    ...(isAdmin
      ? [{
          key: 'grp-meta',
          type: 'group' as const,
          label: '元数据',
          children: [
            { key: '/conflicts', icon: <WarningOutlined />, label: '冲突管理' },
          ],
        }]
      : []),
    // ── 性能 ──
    {
      key: 'grp-perf',
      type: 'group',
      label: '性能',
      children: [{ key: '/kv', icon: <KeyOutlined />, label: 'KV 管理' }],
    },
    // ── 安全 ──
    {
      key: 'grp-security',
      type: 'group',
      label: '安全',
      children: [
        { key: '/access-keys', icon: <LockOutlined />, label: '我的密钥' },
        ...(isAdmin
          ? [
              { key: '/users', icon: <TeamOutlined />, label: '用户管理' },
              { key: '/roles', icon: <SafetyCertificateOutlined />, label: '角色管理' },
            ]
          : []),
      ],
    },
  ]

  const handleLogout = () => {
    authLogout()
    message.success('已退出登录')
    navigate('/login', { replace: true })
  }

  const userMenuItems: MenuProps['items'] = [
    {
      key: 'user-info',
      label: (
        <div style={{ padding: '4px 8px' }}>
          <div style={{ fontWeight: 500 }}>{user?.username ?? '-'}</div>
          <Text type="secondary" style={{ fontSize: 12 }}>
            {user?.role === 'admin' ? '管理员' : '普通用户'}
          </Text>
        </div>
      ),
      disabled: true,
    },
    { type: 'divider' },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: '退出登录',
      onClick: handleLogout,
    },
  ]

  const themeMenuItems: MenuProps['items'] = [
    {
      key: 'light',
      icon: <BulbOutlined />,
      label: '明亮',
      onClick: () => setMode('light' as ThemeMode),
    },
    {
      key: 'dark',
      icon: <BulbFilled />,
      label: '暗黑',
      onClick: () => setMode('dark' as ThemeMode),
    },
    {
      key: 'auto',
      icon: <DesktopOutlined />,
      label: '跟随系统',
      onClick: () => setMode('auto' as ThemeMode),
    },
  ]

  const themeLabel = mode === 'light' ? '明亮' : mode === 'dark' ? '暗黑' : '自动'

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
            padding: '20px 16px',
            textAlign: 'center',
            borderBottom: '1px solid var(--pf-sider-border)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 10,
          }}
        >
          <RocketOutlined style={{ fontSize: 24, color: 'var(--pf-color-brand)' }} />
          {!collapsed && (
            <span
              className="pf-gradient-text"
              style={{ fontSize: 20, fontWeight: 700, letterSpacing: 0.5 }}
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
            <Text strong style={{ fontSize: 16 }}>监控管理平台</Text>
          </Space>

          <Space size={16}>
            {/* Cluster health badge */}
            <Tooltip title="集群当前健康状态">
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
                系统运行正常
              </Tag>
            </Tooltip>

            {/* Global search trigger */}
            <Tooltip title="全局搜索 (Ctrl+K)">
              <Button
                type="text"
                icon={<SearchOutlined />}
                onClick={() => searchRef.current?.open()}
              />
            </Tooltip>

            {/* Theme switcher */}
            <Dropdown menu={{ items: themeMenuItems }} placement="bottomRight">
              <Tooltip title={`主题: ${themeLabel}`}>
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
                <span>{user?.username ?? '未登录'}</span>
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
