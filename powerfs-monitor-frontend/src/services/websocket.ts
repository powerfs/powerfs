/**
 * WebSocket service for /ws/metrics.
 *
 * The backend pushes two kinds of messages:
 *   - { type: 'metric_update', source: 'nodes'|'volumes'|'kv'|..., payload: {...} }
 *   - { type: 'alert_trigger'|'alert_resolve', payload: {...} }
 *
 * This module is a singleton with ref-counted subscribers so that multiple
 * hooks on different pages share one connection, and the socket is only
 * closed when the last subscriber unsubscribes.
 */

export interface MetricUpdate {
  type: 'metric_update'
  source: string
  payload: Record<string, unknown> | unknown
}

export interface AlertUpdate {
  type: 'alert_trigger' | 'alert_resolve'
  payload: Record<string, unknown>
}

export type WsMessage = MetricUpdate | AlertUpdate

type Listener = (msg: WsMessage) => void

const WS_RECONNECT_BASE_MS = 1000
const WS_RECONNECT_MAX_MS = 30000
const WS_HEARTBEAT_MS = 25000

let socket: WebSocket | null = null
let refCount = 0
let listeners = new Set<Listener>()
let reconnectAttempts = 0
let reconnectTimer: ReturnType<typeof setTimeout> | null = null
let heartbeatTimer: ReturnType<typeof setInterval> | null = null
let manualClose = false

function buildWsUrl(): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/ws/metrics`
}

function scheduleReconnect() {
  if (manualClose) return
  if (reconnectTimer) clearTimeout(reconnectTimer)
  const delay = Math.min(
    WS_RECONNECT_BASE_MS * 2 ** reconnectAttempts,
    WS_RECONNECT_MAX_MS,
  )
  reconnectAttempts++
  reconnectTimer = setTimeout(() => {
    connect()
  }, delay)
}

function startHeartbeat() {
  if (heartbeatTimer) clearInterval(heartbeatTimer)
  // Browsers auto-send ping frames on idle; we send an app-level ping to
  // also keep proxies / load balancers warm. Server ignores text "ping".
  heartbeatTimer = setInterval(() => {
    if (socket?.readyState === WebSocket.OPEN) {
      try {
        socket.send('ping')
      } catch {
        /* ignore — onclose will handle reconnect */
      }
    }
  }, WS_HEARTBEAT_MS)
}

function stopHeartbeat() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer)
    heartbeatTimer = null
  }
}

function dispatch(msg: WsMessage) {
  for (const l of listeners) {
    try {
      l(msg)
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error('WS listener threw:', e)
    }
  }
}

function handleOpen() {
  reconnectAttempts = 0
  startHeartbeat()
}

function handleMessage(event: MessageEvent) {
  // Server heartbeat ack is ignored.
  if (event.data === 'pong' || event.data === 'ping') return
  try {
    const data = JSON.parse(event.data) as WsMessage
    if (
      data?.type === 'metric_update' ||
      data?.type === 'alert_trigger' ||
      data?.type === 'alert_resolve'
    ) {
      dispatch(data)
    }
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('Failed to parse WS message:', e)
  }
}

function handleClose() {
  stopHeartbeat()
  socket = null
  if (!manualClose) {
    scheduleReconnect()
  }
}

function handleError() {
  // onclose will be called after onerror; do nothing here to avoid double reconnect.
}

function connect() {
  if (typeof window === 'undefined') return
  if (socket?.readyState === WebSocket.OPEN || socket?.readyState === WebSocket.CONNECTING) {
    return
  }
  manualClose = false
  try {
    socket = new WebSocket(buildWsUrl())
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('WebSocket construction failed:', e)
    scheduleReconnect()
    return
  }
  socket.onopen = handleOpen
  socket.onmessage = handleMessage
  socket.onclose = handleClose
  socket.onerror = handleError
}

function disconnect() {
  manualClose = true
  if (reconnectTimer) {
    clearTimeout(reconnectTimer)
    reconnectTimer = null
  }
  stopHeartbeat()
  if (socket) {
    socket.onclose = null
    socket.onerror = null
    socket.onmessage = null
    socket.onopen = null
    try {
      socket.close()
    } catch {
      /* noop */
    }
    socket = null
  }
}

/**
 * Subscribe to WebSocket messages. The socket is created on the first
 * subscriber and torn down when the last subscriber leaves.
 * Returns an unsubscribe function.
 */
export function subscribe(listener: Listener): () => void {
  listeners.add(listener)
  refCount++
  if (refCount === 1) {
    connect()
  }
  return () => {
    listeners.delete(listener)
    refCount = Math.max(0, refCount - 1)
    if (refCount === 0) {
      disconnect()
    }
  }
}

/** Connection status for UI affordances. */
export function getWsStatus(): 'connecting' | 'open' | 'closed' {
  if (!socket) return 'closed'
  switch (socket.readyState) {
    case WebSocket.OPEN:
      return 'open'
    case WebSocket.CONNECTING:
      return 'connecting'
    default:
      return 'closed'
  }
}

/** For testing: force a reconnect (e.g. after auth change). */
export function forceReconnect() {
  manualClose = false
  disconnect()
  manualClose = false
  if (refCount > 0) connect()
}

/** Legacy API kept for any non-hook callers. */
export function connectWebSocket(
  onMetricUpdate?: (data: MetricUpdate) => void,
  onAlertUpdate?: (data: AlertUpdate) => void,
): () => void {
  return subscribe((msg) => {
    if (msg.type === 'metric_update') {
      onMetricUpdate?.(msg)
    } else if (msg.type === 'alert_trigger' || msg.type === 'alert_resolve') {
      onAlertUpdate?.(msg as AlertUpdate)
    }
  })
}

export function disconnectWebSocket() {
  // Drop all subscribers (only safe to call from app teardown).
  listeners.clear()
  refCount = 0
  disconnect()
}
