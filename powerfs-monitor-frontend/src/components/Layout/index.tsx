import { useEffect, useState } from 'react'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { Layout, Menu, Button, Space, Dropdown, Avatar, Typography, message } from 'antd'
import type { MenuProps } from 'antd'
import {
  DashboardOutlined,
  DatabaseOutlined,
  KeyOutlined,
  BellOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  RocketOutlined,
  SaveOutlined,
  CloudOutlined,
  FolderOpenOutlined,
  UserOutlined,
  LogoutOutlined,
  TeamOutlined,
  SafetyCertificateOutlined,
  LockOutlined,
  WarningOutlined,
} from '@ant-design/icons'
import {
  subscribe,
  getCurrentUser,
  logout as authLogout,
  type CurrentUser,
} from '@/services/auth'

const { Header, Sider, Content } = Layout
const { Text } = Typography

function AppLayout() {
  const [collapsed, setCollapsed] = useState(false)
  const location = useLocation()
  const navigate = useNavigate()
  const [user, setUser] = useState<CurrentUser | null>(getCurrentUser())

  useEffect(() => {
    const unsubscribe = subscribe(() => {
      setUser(getCurrentUser())
    })
    return unsubscribe
  }, [])

  const isAdmin = user?.role === 'admin'

  const menuItems = [
    ...(isAdmin
      ? [
          { key: '/', icon: <DashboardOutlined />, label: '仪表盘' },
          { key: '/nodes', icon: <SaveOutlined />, label: '节点管理' },
          { key: '/volumes', icon: <DatabaseOutlined />, label: 'Volume管理' },
          { key: '/fuse', icon: <FolderOpenOutlined />, label: 'FUSE管理' },
          { key: '/conflicts', icon: <WarningOutlined />, label: '冲突管理' },
        ]
      : []),
    { key: '/kv', icon: <KeyOutlined />, label: 'KV管理' },
    { key: '/s3', icon: <CloudOutlined />, label: 'S3管理' },
    { key: '/alerts', icon: <BellOutlined />, label: '告警中心' },
    { key: '/access-keys', icon: <LockOutlined />, label: '我的密钥' },
    ...(isAdmin
      ? [
          { key: '/users', icon: <TeamOutlined />, label: '用户管理' },
          { key: '/roles', icon: <SafetyCertificateOutlined />, label: '角色管理' },
        ]
      : []),
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

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        theme="dark"
        style={{
          background: 'linear-gradient(180deg, #1a1a2e 0%, #16213e 100%)',
        }}
      >
        <div style={{ padding: '20px 16px', textAlign: 'center', borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
          <Space align="center" style={{ justifyContent: 'center' }}>
            <RocketOutlined style={{ fontSize: 24, color: '#1890ff' }} />
            {!collapsed && (
              <span style={{ color: '#fff', fontSize: 18, fontWeight: 'bold' }}>PowerFS</span>
            )}
          </Space>
        </div>
        <Menu
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{
            background: 'transparent',
            borderRight: 'none',
          }}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            background: '#fff',
            padding: '0 24px',
            boxShadow: '0 2px 8px rgba(0,0,0,0.06)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <Button
            type="text"
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={() => setCollapsed(!collapsed)}
            style={{ marginRight: 16 }}
          />
          <span style={{ fontSize: 18, fontWeight: 500 }}>监控管理平台</span>
          <Space>
            <span style={{ color: '#52c41a' }}>● 系统运行正常</span>
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
            background: '#f0f2f5',
            borderRadius: 8,
          }}
        >
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  )
}

export default AppLayout