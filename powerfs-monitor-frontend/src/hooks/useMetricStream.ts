import { useEffect, useRef, useState } from 'react'
import {
  subscribe,
  getWsStatus,
  type MetricUpdate,
  type AlertUpdate,
  type WsMessage,
} from '@/services/websocket'

type Status = 'connecting' | 'open' | 'closed'

export interface UseMetricStreamOptions {
  /**
   * If provided, only MetricUpdates with this `source` are delivered to
   * `onMetricUpdate`. Pass undefined to receive all sources.
   */
  source?: string
  /** Called for each matching MetricUpdate (live data, full payload). */
  onMetricUpdate?: (update: MetricUpdate) => void
  /** Called for alert trigger/resolve events. */
  onAlertUpdate?: (update: AlertUpdate) => void
}

export interface UseMetricStreamResult {
  /** Current WebSocket connection status. */
  status: Status
  /** Timestamp (ms) of the last message received; 0 if none. */
  lastUpdated: number
  /** The most recent matching message (for direct rendering). */
  lastMessage: MetricUpdate | null
}

/**
 * Subscribe to /ws/metrics. The underlying WebSocket is ref-counted and
 * shared across all active hook instances, so mounting multiple pages
 * that use this hook only opens one connection.
 *
 * Usage:
 *   const { status, lastUpdated } = useMetricStream({
 *     source: 'nodes',
 *     onMetricUpdate: (u) => setNodes(prev => mergeById(prev, u.payload as NodeInfo)),
 *   })
 */
export function useMetricStream(
  options: UseMetricStreamOptions = {},
): UseMetricStreamResult {
  const { source, onMetricUpdate, onAlertUpdate } = options
  const [status, setStatus] = useState<Status>('closed')
  const [lastUpdated, setLastUpdated] = useState(0)
  const [lastMessage, setLastMessage] = useState<MetricUpdate | null>(null)

  // Keep latest callbacks in refs so the subscription doesn't need to be
  // re-created on every render (avoids socket churn).
  const metricCbRef = useRef(onMetricUpdate)
  const alertCbRef = useRef(onAlertUpdate)
  const sourceRef = useRef(source)
  metricCbRef.current = onMetricUpdate
  alertCbRef.current = onAlertUpdate
  sourceRef.current = source

  useEffect(() => {
    const unsubscribe = subscribe((msg: WsMessage) => {
      if (msg.type === 'metric_update') {
        const m = msg as MetricUpdate
        if (sourceRef.current !== undefined && m.source !== sourceRef.current) {
          return
        }
        setLastMessage(m)
        setLastUpdated(Date.now())
        metricCbRef.current?.(m)
      } else {
        // alert_trigger | alert_resolve
        alertCbRef.current?.(msg as AlertUpdate)
        setLastUpdated(Date.now())
      }
    })

    // Poll connection status cheaply. The socket may still be CONNECTING
    // when this effect first runs; sampling every 1s gives a snappy UI
    // without flooding React with renders.
    const statusTimer = setInterval(() => {
      setStatus(getWsStatus())
    }, 1000)
    setStatus(getWsStatus())

    return () => {
      unsubscribe()
      clearInterval(statusTimer)
    }
  }, [])

  return { status, lastUpdated, lastMessage }
}

export default useMetricStream
