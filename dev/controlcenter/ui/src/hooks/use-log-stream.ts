import { useState, useEffect, useRef } from 'react'
import { api, type LogEntry } from '@/lib/api'

const RECONNECT_INITIAL_MS = 1000
const RECONNECT_MAX_MS = 30000

export function useLogStream() {
  const [streamedLogs, setStreamedLogs] = useState<LogEntry[]>([])
  const [connected, setConnected] = useState(false)
  // Use a ref so the reconnect closure always sees the latest "should we still be running" flag
  const activeRef = useRef(true)

  useEffect(() => {
    activeRef.current = true
    let eventSource: EventSource | null = null
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null
    let reconnectDelay = RECONNECT_INITIAL_MS

    function connect() {
      if (!activeRef.current) return

      eventSource = api.createLogStream('all', 'all')

      eventSource.onopen = () => {
        setConnected(true)
        reconnectDelay = RECONNECT_INITIAL_MS // reset backoff on success
      }

      eventSource.onmessage = (e) => {
        const entry: LogEntry = JSON.parse(e.data)
        setStreamedLogs((prev) => {
          const next = [...prev, entry]
          return next.length > 10_000 ? next.slice(next.length - 10_000) : next
        })
      }

      eventSource.onerror = () => {
        setConnected(false)
        eventSource?.close()
        eventSource = null
        if (!activeRef.current) return
        // Reconnect with exponential backoff
        reconnectTimer = setTimeout(() => {
          reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS)
          connect()
        }, reconnectDelay)
      }
    }

    connect()

    return () => {
      activeRef.current = false
      if (reconnectTimer) clearTimeout(reconnectTimer)
      eventSource?.close()
    }
  }, [])

  const clearStreamedLogs = () => setStreamedLogs([])

  return { streamedLogs, connected, clearStreamedLogs }
}
