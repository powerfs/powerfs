/**
 * EmptyState — friendly empty-data placeholder with optional illustration
 * slot and call-to-action button.
 *
 * Use this instead of antd's default "暂无数据" to give users clear next steps.
 */

import React from 'react'
import { Empty, Button, Typography } from 'antd'

const { Text } = Typography

export interface EmptyStateProps {
  title?: string
  description?: React.ReactNode
  icon?: React.ReactNode
  ctaText?: string
  onCtaClick?: () => void
  style?: React.CSSProperties
}

const EmptyState: React.FC<EmptyStateProps> = ({
  title = '暂无数据',
  description,
  icon,
  ctaText,
  onCtaClick,
  style,
}) => {
  return (
    <div
      style={{
        padding: '48px 24px',
        textAlign: 'center',
        color: 'var(--pf-color-text-tertiary)',
        ...style,
      }}
    >
      {icon ? (
        <div style={{ fontSize: 48, marginBottom: 16, opacity: 0.6 }}>{icon}</div>
      ) : (
        <Empty
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          description={false}
          style={{ marginBottom: 16 }}
        />
      )}
      <div style={{ fontSize: 16, fontWeight: 500, marginBottom: 4 }}>
        {title}
      </div>
      {description && (
        <Text type="secondary" style={{ fontSize: 13 }}>
          {description}
        </Text>
      )}
      {ctaText && onCtaClick && (
        <div style={{ marginTop: 16 }}>
          <Button type="primary" onClick={onCtaClick}>
            {ctaText}
          </Button>
        </div>
      )}
    </div>
  )
}

export default EmptyState