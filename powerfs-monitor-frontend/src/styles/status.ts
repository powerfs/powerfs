/**
 * PowerFS Status Color System
 *
 * Unified color mapping for node/device/volume/task statuses.
 * Each status has 4 levels (bg / border / text / dot) to keep visual
 * consistency across Tag, Badge, TopologyNode, and Table columns.
 *
 * Node status follows PowerFS native state machine:
 * (online | degraded | maintenance | isolated | offline | initializing)
 */

export type NodeStatus =
  | 'online'
  | 'degraded'
  | 'maintenance'
  | 'isolated'
  | 'offline'
  | 'initializing'

export type DeviceStatus =
  | 'online'
  | 'offline'
  | 'excluded'
  | 'draining'
  | 'faulty'

export type VolumeStatus = 'available' | 'full' | 'readonly' | 'creating'

export type CircuitBreakerStatus = 'closed' | 'open' | 'half_open'

export type TaskStatus =
  | 'pending'
  | 'running'
  | 'paused'
  | 'completed'
  | 'failed'
  | 'cancelled'

export type AlertSeverity = 'info' | 'warning' | 'critical'

interface StatusPalette {
  /** Background fill (light) */
  bg: string
  /** Border color */
  border: string
  /** Foreground text color */
  text: string
  /** Solid dot/icon color (for badges, topology nodes) */
  dot: string
  /** Antd Tag color name or hex (used as shorthand) */
  tag: string
  /** Human-readable label (Chinese) */
  label: string
}

/** Node status palette (PowerFS native state machine) */
export const nodeStatusPalette: Record<NodeStatus, StatusPalette> = {
  online: {
    bg: '#F6FFED',
    border: '#B7EB8F',
    text: '#389E0D',
    dot: '#52C41A',
    tag: 'success',
    label: '在线',
  },
  degraded: {
    bg: '#FFF7E6',
    border: '#FFD591',
    text: '#D46B08',
    dot: '#FAAD14',
    tag: 'orange',
    label: '降级',
  },
  maintenance: {
    bg: '#F0F5FF',
    border: '#ADC6FF',
    text: '#1F3B94',
    dot: '#4080FF',
    tag: 'blue',
    label: '维护中',
  },
  isolated: {
    bg: '#FFF1F0',
    border: '#FFA39E',
    text: '#CF1322',
    dot: '#FF4D4F',
    tag: 'red',
    label: '已隔离',
  },
  offline: {
    bg: '#F5F5F5',
    border: '#D9D9D9',
    text: '#595959',
    dot: '#8C8C8C',
    tag: 'default',
    label: '离线',
  },
  initializing: {
    bg: '#E6F7FF',
    border: '#91D5FF',
    text: '#096DD9',
    dot: '#1890FF',
    tag: 'cyan',
    label: '初始化',
  },
}

export type RaftRole = 'leader' | 'follower'

export const raftRolePalette: Record<RaftRole, StatusPalette> = {
  leader: {
    bg: '#F6FFED',
    border: '#B7EB8F',
    text: '#389E0D',
    dot: '#52C41A',
    tag: 'success',
    label: 'Leader',
  },
  follower: {
    bg: '#E6F7FF',
    border: '#91D5FF',
    text: '#096DD9',
    dot: '#1890FF',
    tag: 'blue',
    label: 'Follower',
  },
}

/** Device status palette */
export const deviceStatusPalette: Record<DeviceStatus, StatusPalette> = {
  online: {
    bg: '#F6FFED',
    border: '#B7EB8F',
    text: '#389E0D',
    dot: '#52C41A',
    tag: 'success',
    label: '在线',
  },
  offline: {
    bg: '#F5F5F5',
    border: '#D9D9D9',
    text: '#595959',
    dot: '#8C8C8C',
    tag: 'default',
    label: '离线',
  },
  excluded: {
    bg: '#F9F0FF',
    border: '#D3ADF7',
    text: '#531DAB',
    dot: '#722ED1',
    tag: 'purple',
    label: '已排除',
  },
  draining: {
    bg: '#E6F7FF',
    border: '#91D5FF',
    text: '#096DD9',
    dot: '#1890FF',
    tag: 'blue',
    label: '驱逐中',
  },
  faulty: {
    bg: '#FFF1F0',
    border: '#FFA39E',
    text: '#CF1322',
    dot: '#FF4D4F',
    tag: 'red',
    label: '故障',
  },
}

/** Volume status palette */
export const volumeStatusPalette: Record<VolumeStatus, StatusPalette> = {
  available: {
    bg: '#F6FFED',
    border: '#B7EB8F',
    text: '#389E0D',
    dot: '#52C41A',
    tag: 'success',
    label: '可用',
  },
  full: {
    bg: '#FFF1F0',
    border: '#FFA39E',
    text: '#CF1322',
    dot: '#FF4D4F',
    tag: 'red',
    label: '已满',
  },
  readonly: {
    bg: '#E6F7FF',
    border: '#91D5FF',
    text: '#096DD9',
    dot: '#1890FF',
    tag: 'blue',
    label: '只读',
  },
  creating: {
    bg: '#FFF7E6',
    border: '#FFD591',
    text: '#D46B08',
    dot: '#FAAD14',
    tag: 'orange',
    label: '创建中',
  },
}

/** Circuit breaker palette */
export const circuitBreakerPalette: Record<CircuitBreakerStatus, StatusPalette> = {
  closed: {
    bg: '#F6FFED',
    border: '#B7EB8F',
    text: '#389E0D',
    dot: '#52C41A',
    tag: 'success',
    label: '闭合',
  },
  open: {
    bg: '#FFF1F0',
    border: '#FFA39E',
    text: '#CF1322',
    dot: '#FF4D4F',
    tag: 'red',
    label: '熔断',
  },
  half_open: {
    bg: '#FFF7E6',
    border: '#FFD591',
    text: '#D46B08',
    dot: '#FAAD14',
    tag: 'orange',
    label: '半开',
  },
}

/** Task status palette */
export const taskStatusPalette: Record<TaskStatus, StatusPalette> = {
  pending: {
    bg: '#FFF7E6',
    border: '#FFD591',
    text: '#D46B08',
    dot: '#FAAD14',
    tag: 'orange',
    label: '等待中',
  },
  running: {
    bg: '#E6F7FF',
    border: '#91D5FF',
    text: '#096DD9',
    dot: '#1890FF',
    tag: 'processing',
    label: '运行中',
  },
  paused: {
    bg: '#F5F5F5',
    border: '#D9D9D9',
    text: '#595959',
    dot: '#8C8C8C',
    tag: 'default',
    label: '已暂停',
  },
  completed: {
    bg: '#F6FFED',
    border: '#B7EB8F',
    text: '#389E0D',
    dot: '#52C41A',
    tag: 'success',
    label: '已完成',
  },
  failed: {
    bg: '#FFF1F0',
    border: '#FFA39E',
    text: '#CF1322',
    dot: '#FF4D4F',
    tag: 'red',
    label: '失败',
  },
  cancelled: {
    bg: '#F5F5F5',
    border: '#D9D9D9',
    text: '#595959',
    dot: '#8C8C8C',
    tag: 'default',
    label: '已取消',
  },
}

/** Alert severity palette */
export const alertSeverityPalette: Record<AlertSeverity, StatusPalette> = {
  info: {
    bg: '#E6F7FF',
    border: '#91D5FF',
    text: '#096DD9',
    dot: '#1890FF',
    tag: 'blue',
    label: '信息',
  },
  warning: {
    bg: '#FFF7E6',
    border: '#FFD591',
    text: '#D46B08',
    dot: '#FAAD14',
    tag: 'orange',
    label: '警告',
  },
  critical: {
    bg: '#FFF1F0',
    border: '#FFA39E',
    text: '#CF1322',
    dot: '#FF4D4F',
    tag: 'red',
    label: '严重',
  },
}

/**
 * Resolve a status string (possibly unknown) to a palette.
 * Directly maps PowerFS native status values to their color palettes.
 */
export function resolveNodeStatus(status: string): StatusPalette {
  return (nodeStatusPalette as Record<string, StatusPalette>)[status] ?? {
    bg: '#F5F5F5',
    border: '#D9D9D9',
    text: '#595959',
    dot: '#8C8C8C',
    tag: 'default',
    label: status || '未知',
  }
}

export function resolveDeviceStatus(status: string): StatusPalette {
  return (deviceStatusPalette as Record<string, StatusPalette>)[status] ?? {
    bg: '#F5F5F5',
    border: '#D9D9D9',
    text: '#595959',
    dot: '#8C8C8C',
    tag: 'default',
    label: status || '未知',
  }
}

export function resolveVolumeStatus(status: string): StatusPalette {
  return (volumeStatusPalette as Record<string, StatusPalette>)[status] ?? {
    bg: '#F5F5F5',
    border: '#D9D9D9',
    text: '#595959',
    dot: '#8C8C8C',
    tag: 'default',
    label: status || '未知',
  }
}