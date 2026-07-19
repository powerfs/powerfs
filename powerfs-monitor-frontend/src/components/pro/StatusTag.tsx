/**
 * StatusTag — unified status badge with dot indicator.
 *
 * Reads from `status.ts` palette so colors stay consistent across pages.
 */

import React from 'react'
import { Tag } from 'antd'
import {
  nodeStatusPalette,
  deviceStatusPalette,
  volumeStatusPalette,
  circuitBreakerPalette,
  taskStatusPalette,
  alertSeverityPalette,
  resolveNodeStatus,
  resolveDeviceStatus,
  resolveVolumeStatus,
  type NodeStatus,
  type DeviceStatus,
  type VolumeStatus,
  type CircuitBreakerStatus,
  type TaskStatus,
  type AlertSeverity,
} from '@/styles/status'

type StatusKind =
  | 'node'
  | 'device'
  | 'volume'
  | 'circuit'
  | 'task'
  | 'alert'
  | 'auto'

export interface StatusTagProps {
  status: string
  /** Which palette to use. 'auto' tries node → device → volume. */
  kind?: StatusKind
  /** Show pulsing dot (good for live states) */
  pulse?: boolean
  /** Override label text; defaults to palette label */
  label?: string
  style?: React.CSSProperties
}

function pickPalette(
  kind: StatusKind,
  status: string,
): { bg: string; border: string; text: string; dot: string; label: string } {
  switch (kind) {
    case 'node':
      return resolveNodeStatus(status)
    case 'device':
      return resolveDeviceStatus(status)
    case 'volume':
      return resolveVolumeStatus(status)
    case 'circuit':
      return (
        (circuitBreakerPalette as Record<string, ReturnType<typeof resolveNodeStatus>>)[status] ?? {
          bg: '#F5F5F5',
          border: '#D9D9D9',
          text: '#595959',
          dot: '#8C8C8C',
          label: status || '未知',
        }
      )
    case 'task':
      return (
        (taskStatusPalette as Record<string, ReturnType<typeof resolveNodeStatus>>)[status] ?? {
          bg: '#F5F5F5',
          border: '#D9D9D9',
          text: '#595959',
          dot: '#8C8C8C',
          label: status || '未知',
        }
      )
    case 'alert':
      return (
        (alertSeverityPalette as Record<string, ReturnType<typeof resolveNodeStatus>>)[status] ?? {
          bg: '#F5F5F5',
          border: '#D9D9D9',
          text: '#595959',
          dot: '#8C8C8C',
          label: status || '未知',
        }
      )
    case 'auto':
    default:
      return (
        (nodeStatusPalette as Record<string, ReturnType<typeof resolveNodeStatus>>)[status] ??
        (deviceStatusPalette as Record<string, ReturnType<typeof resolveNodeStatus>>)[status] ??
        (volumeStatusPalette as Record<string, ReturnType<typeof resolveNodeStatus>>)[status] ?? {
          bg: '#F5F5F5',
          border: '#D9D9D9',
          text: '#595959',
          dot: '#8C8C8C',
          label: status || '未知',
        }
      )
  }
}

const StatusTag: React.FC<StatusTagProps> = ({
  status,
  kind = 'auto',
  pulse = false,
  label,
  style,
}) => {
  const palette = pickPalette(kind, status)

  return (
    <Tag
      style={{
        background: palette.bg,
        borderColor: palette.border,
        color: palette.text,
        borderRadius: 6,
        padding: '2px 10px',
        fontSize: 12,
        lineHeight: '20px',
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        ...style,
      }}
    >
      <span
        className={pulse ? 'pf-pulse' : ''}
        style={{
          width: 6,
          height: 6,
          borderRadius: '50%',
          background: palette.dot,
          display: 'inline-block',
        }}
      />
      {label ?? palette.label}
    </Tag>
  )
}

export default StatusTag

// Re-export status types for convenience
export type {
  NodeStatus,
  DeviceStatus,
  VolumeStatus,
  CircuitBreakerStatus,
  TaskStatus,
  AlertSeverity,
}
