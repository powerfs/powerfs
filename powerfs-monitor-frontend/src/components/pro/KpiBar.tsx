/**
 * KpiBar — horizontal strip of KPI cards used at the top of dashboards.
 *
 * Renders an array of StatCard-like entries in a responsive grid that
 * collapses from 4-up → 2-up → 1-up depending on viewport.
 */

import React from 'react'
import { Row, Col } from 'antd'
import StatCard, { type StatCardProps } from './StatCard'

export interface KpiBarProps {
  items: StatCardProps[]
  gutter?: [number, number]
}

const KpiBar: React.FC<KpiBarProps> = ({ items, gutter = [16, 16] }) => {
  return (
    <Row gutter={gutter}>
      {items.map((item, idx) => (
        <Col key={idx} xs={24} sm={12} lg={6}>
          <StatCard {...item} />
        </Col>
      ))}
    </Row>
  )
}

export default KpiBar