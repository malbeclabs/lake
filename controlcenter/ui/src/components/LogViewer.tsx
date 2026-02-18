import { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import { VariableSizeList as List } from 'react-window'
import { type LogEntry, type LogLevel } from '@/lib/api'
import { cn } from '@/lib/utils'

const CHARS_PER_LINE = 110
const BASE_HEIGHT = 36
const LINE_HEIGHT = 20

function estimateHeight(log: LogEntry): number {
  const newlineCount = (log.line.match(/\n/g) || []).length
  const wrappedLines = Math.ceil(log.line.length / CHARS_PER_LINE) - 1
  const extraLines = Math.max(newlineCount, wrappedLines)
  return BASE_HEIGHT + extraLines * LINE_HEIGHT
}

interface LogViewerProps {
  logs: LogEntry[]
  serviceFilter: string
  levelFilter: string
  keywordFilter: string
  autoScroll?: boolean
}

export function LogViewer({
  logs,
  serviceFilter,
  levelFilter,
  keywordFilter,
  autoScroll = true,
}: LogViewerProps) {
  const listRef = useRef<List>(null)
  const [isPaused, setIsPaused] = useState(false)

  // Apply all filters client-side, memoised so the array is stable between unrelated renders
  const filteredLogs = useMemo(() => logs.filter((log) => {
    if (serviceFilter && serviceFilter !== 'all' && log.service !== serviceFilter) return false
    if (levelFilter && levelFilter !== 'all' && log.level !== levelFilter) return false
    if (keywordFilter && !log.line.toLowerCase().includes(keywordFilter.toLowerCase())) return false
    return true
  }), [logs, serviceFilter, levelFilter, keywordFilter])

  // Compute heights for each row, memoised alongside filteredLogs
  const heights = useMemo(() => filteredLogs.map(estimateHeight), [filteredLogs])

  const getItemSize = useCallback((index: number) => heights[index] ?? BASE_HEIGHT, [heights])

  // Reset list measurements whenever filteredLogs changes (content OR length)
  useEffect(() => {
    listRef.current?.resetAfterIndex(0)
  }, [filteredLogs])

  // Auto-scroll to bottom when new logs arrive
  useEffect(() => {
    if (autoScroll && !isPaused && listRef.current && filteredLogs.length > 0) {
      listRef.current.scrollToItem(filteredLogs.length - 1, 'end')
    }
  }, [filteredLogs.length, autoScroll, isPaused])

  const getLevelClass = (level: LogLevel): string => {
    switch (level) {
      case 'ERROR':
        return 'log-error'
      case 'WARN':
        return 'log-warn'
      case 'INFO':
        return 'log-info'
      case 'DEBUG':
        return 'log-debug'
      default:
        return 'log-unknown'
    }
  }

  const Row = ({ index, style }: { index: number; style: React.CSSProperties }) => {
    const log = filteredLogs[index]
    if (!log) return <div style={style} />
    const timestamp = new Date(log.timestamp).toLocaleTimeString()

    return (
      <div
        style={{ ...style, paddingBottom: 4 }}
        className="font-mono text-sm px-4 py-1 hover:bg-accent/50 flex gap-3"
      >
        <span className="text-muted-foreground shrink-0">{timestamp}</span>
        <span className="text-muted-foreground shrink-0 w-16">
          [{log.service}]
        </span>
        <span className={cn('shrink-0 w-12', getLevelClass(log.level))}>
          {log.level !== 'UNKNOWN' ? log.level : ''}
        </span>
        <span className="break-all whitespace-pre-wrap">{log.line}</span>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-full bg-card border border-border rounded-lg overflow-hidden transition-colors">
      <div className="flex items-center justify-between px-4 py-2 border-b border-border bg-muted transition-colors">
        <div className="text-sm text-muted-foreground">
          {filteredLogs.length} log{filteredLogs.length !== 1 ? 's' : ''}
        </div>
        <button
          onClick={() => setIsPaused(!isPaused)}
          className="text-sm px-3 py-1 rounded-md bg-primary text-primary-foreground hover:bg-primary/90"
        >
          {isPaused ? 'Resume' : 'Pause'} Auto-scroll
        </button>
      </div>

      {filteredLogs.length === 0 ? (
        <div className="flex items-center justify-center h-full text-muted-foreground">
          No logs to display
        </div>
      ) : (
        <List
          ref={listRef}
          height={560}
          itemCount={filteredLogs.length}
          itemSize={getItemSize}
          width="100%"
          className="scrollbar-thin"
        >
          {Row}
        </List>
      )}
    </div>
  )
}
