import { useEffect, useState } from 'react'
import { Card, Typography } from 'antd'

const { Text, Title } = Typography

function Dashboard() {
  const [count, setCount] = useState(0)

  useEffect(() => {
    const timer = setTimeout(() => {
      setCount(1)
    }, 1000)
    return () => clearTimeout(timer)
  }, [])

  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <Title level={4} style={{ margin: 0 }}>集群总览</Title>
        <Text type="secondary">PowerFS 集群实时状态与关键指标</Text>
      </div>

      <Card
        title="测试卡片"
        style={{ borderRadius: 12 }}
        styles={{ body: { padding: 20 } }}
      >
        <div>
          <Text type="secondary">计数器</Text>
          <Text strong style={{ marginLeft: 16 }}>{count}</Text>
        </div>
      </Card>
    </div>
  )
}

export default Dashboard
