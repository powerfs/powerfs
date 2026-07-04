let ws: WebSocket | null = null

export interface MetricUpdate {
  type: string
  source: string
  payload: Record<string, unknown>
}

export interface AlertUpdate {
  type: 'alert_trigger' | 'alert_resolve'
  alert: Record<string, unknown>
}

export function connectWebSocket(onMetricUpdate?: (data: MetricUpdate) => void, onAlertUpdate?: (data: AlertUpdate) => void) {
  if (ws?.readyState === WebSocket.OPEN) {
    return ws
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const wsUrl = `${protocol}//${window.location.host}/ws`
  
  ws = new WebSocket(wsUrl)

  ws.onopen = () => {
    console.log('WebSocket connected')
  }

  ws.onclose = () => {
    console.log('WebSocket disconnected, reconnecting...')
    setTimeout(() => connectWebSocket(onMetricUpdate, onAlertUpdate), 1000)
  }

  ws.onerror = (error) => {
    console.error('WebSocket error:', error)
  }

  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data)
      if (data.type === 'metric_update') {
        onMetricUpdate?.(data)
      } else if (data.type === 'alert_trigger' || data.type === 'alert_resolve') {
        onAlertUpdate?.({ type: data.type, alert: data.payload })
      }
    } catch (e) {
      console.error('Failed to parse WebSocket message:', e)
    }
  }

  return ws
}

export function disconnectWebSocket() {
  ws?.close()
  ws = null
}
