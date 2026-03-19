import { Route, X, ArrowRightLeft } from 'lucide-react'
import type { MultiPathResponse, SinglePath } from '@/lib/api'
import { useTheme } from '@/hooks/use-theme'
import { DeviceSelector, type DeviceOption } from '../DeviceSelector'
import { cn } from '@/lib/utils'

// Path colors for K-shortest paths visualization
const PATH_COLORS = [
  { light: '#16a34a', dark: '#22c55e' },  // green
  { light: '#2563eb', dark: '#3b82f6' },  // blue
  { light: '#9333ea', dark: '#a855f7' },  // purple
  { light: '#ea580c', dark: '#f97316' },  // orange
  { light: '#0891b2', dark: '#06b6d4' },  // cyan
  { light: '#dc2626', dark: '#ef4444' },  // red
  { light: '#ca8a04', dark: '#eab308' },  // yellow
  { light: '#db2777', dark: '#ec4899' },  // pink
  { light: '#059669', dark: '#10b981' },  // emerald
  { light: '#7c3aed', dark: '#8b5cf6' },  // violet
]

interface PathModePanelProps {
  pathSource: string | null
  pathTarget: string | null
  pathsResult: MultiPathResponse | null
  pathLoading: boolean
  selectedPathIndex: number
  devices: DeviceOption[]
  showReverse: boolean
  reversePathsResult: MultiPathResponse | null
  reversePathLoading: boolean
  selectedReversePathIndex: number
  onSelectPath: (index: number) => void
  onSelectReversePath: (index: number) => void
  onClearPath: () => void
  onSetSource: (pk: string | null) => void
  onSetTarget: (pk: string | null) => void
  onToggleReverse: () => void
  pathK: number
  onPathKChange: (k: number) => void
}

export function PathModePanel({
  pathSource,
  pathTarget,
  pathsResult,
  pathLoading,
  selectedPathIndex,
  devices,
  showReverse,
  reversePathsResult,
  reversePathLoading,
  selectedReversePathIndex,
  onSelectPath,
  onSelectReversePath,
  onClearPath,
  onSetSource,
  onSetTarget,
  onToggleReverse,
  pathK,
  onPathKChange,
}: PathModePanelProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  // Get source and target device codes for labels
  const sourceDevice = devices.find(d => d.pk === pathSource)
  const targetDevice = devices.find(d => d.pk === pathTarget)

  // Pick which result set to show based on reverse toggle
  const activeResult = showReverse ? reversePathsResult : pathsResult
  const activeLoading = showReverse ? reversePathLoading : pathLoading
  const activeSelectedIndex = showReverse ? selectedReversePathIndex : selectedPathIndex
  const activeOnSelect = showReverse ? onSelectReversePath : onSelectPath

  const dirLabel = showReverse
    ? `${targetDevice?.code || 'Target'} → ${sourceDevice?.code || 'Source'}`
    : `${sourceDevice?.code || 'Source'} → ${targetDevice?.code || 'Target'}`

  return (
    <div className="p-3 text-xs">
      <div className="flex items-center justify-between mb-2">
        <span className="font-medium flex items-center gap-1.5">
          <Route className="h-3.5 w-3.5 text-amber-500" />
          Device Paths
        </span>
        {(pathSource || pathTarget) && (
          <button onClick={onClearPath} className="p-1 hover:bg-[var(--muted)] rounded" title="Clear path">
            <X className="h-3 w-3" />
          </button>
        )}
      </div>

      {/* Device selectors */}
      <div className="space-y-2 mb-3">
        <DeviceSelector
          devices={devices}
          value={pathSource}
          onChange={onSetSource}
          placeholder="Search source device..."
          label="Source"
          labelColor="#22c55e"
        />
        <DeviceSelector
          devices={devices.filter(d => d.pk !== pathSource)}
          value={pathTarget}
          onChange={onSetTarget}
          placeholder="Search target device..."
          label="Target"
          labelColor="#ef4444"
          disabled={!pathSource}
        />
      </div>

      {!pathSource && (
        <div className="text-muted-foreground text-[10px]">Or click a device on the map</div>
      )}

      {/* Direction toggle + loading */}
      {pathSource && pathTarget && (
        <div className="flex items-center justify-between mb-3">
          <div className="text-[10px] text-muted-foreground uppercase tracking-wider">
            {dirLabel}
          </div>
          <button
            onClick={onToggleReverse}
            className={cn(
              'flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] transition-colors',
              showReverse
                ? 'bg-primary/15 text-primary'
                : 'text-muted-foreground hover:text-foreground hover:bg-[var(--muted)]'
            )}
            title={showReverse ? 'Show forward path' : 'Show reverse path'}
          >
            <ArrowRightLeft className="h-3 w-3" />
            {showReverse ? 'Reverse' : 'Forward'}
          </button>
        </div>
      )}

      {activeLoading && (
        <div className="text-muted-foreground">Finding paths...</div>
      )}

      {/* Path results */}
      {activeResult && !activeResult.error && activeResult.paths.length > 0 && (
        <div>
          {/* Path selector - show if multiple paths */}
          {activeResult.paths.length > 1 && (
            <div className="mb-3">
              <div className="flex items-center gap-1.5 text-muted-foreground mb-1">
                <span>Showing</span>
                <select
                  value={pathK}
                  onChange={e => onPathKChange(Number(e.target.value))}
                  className="bg-muted text-foreground rounded px-1 py-0.5 text-[10px] border border-[var(--border)]"
                >
                  {[3, 5, 10, 15, 20, 25].map(n => (
                    <option key={n} value={n}>{n}</option>
                  ))}
                </select>
              </div>
              <div className="flex flex-wrap gap-1">
                {activeResult.paths.map((_, i) => (
                  <button
                    key={i}
                    onClick={() => activeOnSelect(i)}
                    className={cn(
                      'px-1.5 py-0.5 rounded text-[10px] font-medium transition-colors',
                      activeSelectedIndex === i
                        ? 'bg-primary text-primary-foreground'
                        : 'bg-muted hover:bg-muted/80 text-muted-foreground'
                    )}
                    style={{
                      borderLeft: `3px solid ${isDark ? PATH_COLORS[i % PATH_COLORS.length].dark : PATH_COLORS[i % PATH_COLORS.length].light}`,
                    }}
                  >
                    {i + 1}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Selected path stepper */}
          {activeResult.paths[activeSelectedIndex] && (
            <PathStepper
              path={activeResult.paths[activeSelectedIndex]}
              isReverse={showReverse}
            />
          )}
        </div>
      )}

      {activeResult?.error && (
        <div className="text-destructive">{activeResult.error}</div>
      )}
    </div>
  )
}

// Visual stepper for path hops — matches the path latency page design
function PathStepper({ path, isReverse }: { path: SinglePath; isReverse: boolean }) {
  const isisLatencyMs = path.totalMetric / 1000
  const measuredLatencyMs = path.measuredLatencyMs

  return (
    <div>
      {/* Summary stats */}
      <div className="flex items-center gap-3 mb-3 text-[11px] text-muted-foreground">
        <span>{path.hopCount} hops</span>
        <span className="text-foreground font-medium">{isisLatencyMs.toFixed(2)}ms</span>
        {measuredLatencyMs != null && measuredLatencyMs > 0 && (
          <span className="text-muted-foreground" title="Measured latency">({measuredLatencyMs.toFixed(2)}ms measured)</span>
        )}
      </div>

      {/* Stepper */}
      <div className="space-y-0">
        {path.path.map((hop, idx) => {
          const isFirst = idx === 0
          const isLast = idx === path.path.length - 1
          const nextHop = !isLast ? path.path[idx + 1] : null
          const hopIsisMs = nextHop?.edgeMetric ? nextHop.edgeMetric / 1000 : null
          const hopMeasuredMs = nextHop?.edgeMeasuredMs ?? null

          // Color endpoints: source green, target red (swap for reverse)
          const isSource = isReverse ? isLast : isFirst
          const isTarget = isReverse ? isFirst : isLast

          return (
            <div key={hop.devicePK} className="flex items-stretch gap-3">
              {/* Timeline rail */}
              <div className="flex flex-col items-center w-5 flex-shrink-0">
                <div className={cn(
                  'w-2.5 h-2.5 rounded-full flex-shrink-0 ring-2 ring-background mt-1',
                  isSource ? 'bg-green-500' : isTarget ? 'bg-red-500' : 'bg-muted-foreground/60'
                )} />
                {!isLast && (
                  <div className="w-px flex-1 bg-border min-h-[28px]" />
                )}
              </div>

              {/* Content */}
              <div className={cn('flex-1 flex items-start justify-between', !isLast && 'pb-2')}>
                <div className="flex items-center gap-2 pt-0.5">
                  <span className="font-mono text-[11px] font-medium leading-none">{hop.deviceCode}</span>
                  {hop.metroCode && (
                    <span className="text-[9px] text-muted-foreground bg-muted px-1.5 py-0.5 rounded">
                      {hop.metroCode}
                    </span>
                  )}
                </div>
                {!isLast && hopIsisMs !== null && hopIsisMs > 0 && (
                  <div className="text-right pt-0.5">
                    <span className="text-[10px] font-medium text-primary tabular-nums">
                      {hopIsisMs.toFixed(1)}ms
                    </span>
                    {hopMeasuredMs !== null && hopMeasuredMs > 0 && (
                      <span className="text-[9px] text-muted-foreground tabular-nums block">
                        {hopMeasuredMs.toFixed(1)}ms
                      </span>
                    )}
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
