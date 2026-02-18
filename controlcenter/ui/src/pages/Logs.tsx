import { useState, useEffect, useCallback, useMemo } from 'react'
import { Download, Trash2 } from 'lucide-react'
import { LogViewer } from '@/components/LogViewer'
import { LogFilters } from '@/components/LogFilters'
import { LogHistogram, PRESETS, getIntervalSeconds } from '@/components/LogHistogram'
import { useLogStream } from '@/hooks/use-log-stream'
import { api, type LogEntry, type HistogramBucket } from '@/lib/api'

function makeTimeRange(preset: string): { from: Date; to: Date | null } {
  const p = PRESETS.find((p) => p.label === preset)
  const hours = p ? p.hours : 3
  const from = new Date(Date.now() - hours * 60 * 60 * 1000)
  return { from, to: null }
}

export function Logs() {
  const [service, setService] = useState('all')
  const [level, setLevel] = useState('all')
  const [keyword, setKeyword] = useState('')
  const [preset, setPreset] = useState('3h')
  const [timeFrom, setTimeFrom] = useState<Date>(() => makeTimeRange('3h').from)
  const [timeTo, setTimeTo] = useState<Date | null>(null)

  const [historicalLogs, setHistoricalLogs] = useState<LogEntry[]>([])
  const [histogram, setHistogram] = useState<HistogramBucket[]>([])

  const { streamedLogs, connected, clearStreamedLogs } = useLogStream()

  // Fetch historical logs when timeframe changes
  const fetchHistorical = useCallback(async (from: Date, to: Date | null) => {
    try {
      const fromISO = from.toISOString()
      const toISO = to ? to.toISOString() : undefined
      const entries = await api.getRecentLogs('all', 'all', 5000, fromISO, toISO)
      setHistoricalLogs(entries)
    } catch (e) {
      console.error('Failed to fetch historical logs', e)
    }
  }, [])

  const fetchHistogram = useCallback(async (from: Date, to: Date | null, p: string) => {
    try {
      const fromISO = from.toISOString()
      const toISO = to ? to.toISOString() : new Date().toISOString()
      const hours = PRESETS.find((pr) => pr.label === p)?.hours ?? 3
      const interval = getIntervalSeconds(hours)
      const buckets = await api.getLogHistogram('all', 'all', fromISO, toISO, interval)
      setHistogram(buckets)
    } catch (e) {
      console.error('Failed to fetch histogram', e)
    }
  }, [])

  // Fetch on mount and whenever the timeframe changes
  useEffect(() => {
    fetchHistorical(timeFrom, timeTo)
    fetchHistogram(timeFrom, timeTo, preset)
  }, [timeFrom, timeTo, preset, fetchHistorical, fetchHistogram])

  // Poll histogram every 15 s while the page is open.
  // In live mode (no explicit timeTo) recompute `from` so the window stays
  // relative to now rather than when the page was loaded.
  useEffect(() => {
    const id = setInterval(() => {
      if (timeTo !== null) {
        // Zoomed into a fixed historical range — just refresh that range
        fetchHistogram(timeFrom, timeTo, preset)
      } else {
        // Live mode: slide the window forward
        const { from } = makeTimeRange(preset || '3h')
        fetchHistogram(from, null, preset || '3h')
      }
    }, 15_000)
    return () => clearInterval(id)
  }, [timeFrom, timeTo, preset, fetchHistogram])

  // Merge historical + streamed logs, dedup by timestamp+service+line
  const allLogs = useMemo(() => {
    const seen = new Set<string>()
    const merged: LogEntry[] = []

    for (const log of historicalLogs) {
      const key = `${log.timestamp}|${log.service}|${log.line}`
      if (!seen.has(key)) {
        seen.add(key)
        merged.push(log)
      }
    }

    // Only include streamed logs within the current timeframe
    const rangeStart = timeFrom
    const rangeEnd = timeTo ?? new Date(Date.now() + 60_000) // allow a little future slack

    for (const log of streamedLogs) {
      const t = new Date(log.timestamp)
      if (t < rangeStart || t > rangeEnd) continue
      const key = `${log.timestamp}|${log.service}|${log.line}`
      if (!seen.has(key)) {
        seen.add(key)
        merged.push(log)
      }
    }

    merged.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())
    return merged
  }, [historicalLogs, streamedLogs, timeFrom, timeTo])

  const handlePresetChange = (p: string) => {
    setPreset(p)
    const { from, to } = makeTimeRange(p)
    setTimeFrom(from)
    setTimeTo(to)
  }

  const handleZoom = (from: Date, to: Date) => {
    setPreset('')
    setTimeFrom(from)
    setTimeTo(to)
  }

  const handleClear = () => {
    clearStreamedLogs()
    setHistoricalLogs([])
  }

  const handleDownload = () => {
    const text = allLogs
      .map((log) => `[${log.timestamp}] [${log.service}] [${log.level}] ${log.line}`)
      .join('\n')

    const blob = new Blob([text], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `controlcenter-logs-${Date.now()}.txt`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Service Logs</h1>
          <p className="text-muted-foreground mt-1">
            Real-time log streaming from all services
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleDownload}
            className="flex items-center gap-2 px-4 py-2 rounded-md bg-secondary text-secondary-foreground hover:bg-secondary/80"
          >
            <Download className="w-4 h-4" />
            Download
          </button>
          <button
            onClick={handleClear}
            className="flex items-center gap-2 px-4 py-2 rounded-md bg-destructive text-destructive-foreground hover:bg-destructive/90"
          >
            <Trash2 className="w-4 h-4" />
            Clear
          </button>
        </div>
      </div>

      {/* Connection Status */}
      <div className="flex items-center gap-2 text-sm">
        <div
          className={`w-2 h-2 rounded-full ${
            connected ? 'bg-green-500' : 'bg-red-500'
          }`}
        />
        <span className="text-muted-foreground">
          {connected ? 'Connected to log stream' : 'Disconnected'}
        </span>
      </div>

      {/* Histogram */}
      <LogHistogram
        data={histogram}
        preset={preset}
        onPresetChange={handlePresetChange}
        onZoom={handleZoom}
      />

      {/* Filters */}
      <LogFilters
        service={service}
        level={level}
        keyword={keyword}
        onServiceChange={setService}
        onLevelChange={setLevel}
        onKeywordChange={setKeyword}
      />

      {/* Log Viewer */}
      <LogViewer
        logs={allLogs}
        serviceFilter={service}
        levelFilter={level}
        keywordFilter={keyword}
      />
    </div>
  )
}
