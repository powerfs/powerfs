/**
 * SkeletonCard — placeholder matching StatCard / Card layout during loading.
 * Uses the `.pf-shimmer` CSS class for the gradient sweep animation.
 */

import React from 'react'
import { Card } from 'antd'

export interface SkeletonCardProps {
  height?: number | string
  style?: React.CSSProperties
  /** Render inside a Card container (default true) */
  withCard?: boolean
}

const SkeletonCard: React.FC<SkeletonCardProps> = ({
  height = 120,
  style,
  withCard = true,
}) => {
  const inner = (
    <div
      className="pf-shimmer"
      style={{
        width: '100%',
        height,
        borderRadius: 8,
        ...style,
      }}
    />
  )

  if (!withCard) return inner

  return (
    <Card styles={{ body: { padding: 16 } }} style={{ borderRadius: 12 }}>
      {inner}
    </Card>
  )
}

export default SkeletonCard