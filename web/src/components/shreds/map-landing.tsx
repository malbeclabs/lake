import { useCallback, useMemo, useState, lazy, Suspense } from 'react'
import MapGL, { Marker, NavigationControl } from 'react-map-gl/maplibre'
import type { StyleSpecification } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { Check, Copy, Loader2, Zap, X, ChevronDown, ChevronUp } from 'lucide-react'
import type { Metro, ShredDevice } from '@/lib/api'
import { useTheme } from '@/hooks/use-theme'
import { DevicePicker } from './device-picker'
import type { DeviceLatency, LatencyEntry } from './types'
import type { PickerGlobeMetro } from './picker-globe'

// Lazy-load the picker globe to keep three.js out of the main bundle for
// users on list view or browsers without WebGL.
const PickerGlobe = lazy(() =>
  import('./picker-globe').then(m => ({ default: m.PickerGlobe })),
)

function isWebGLAvailable(): boolean {
  try {
    const canvas = document.createElement('canvas')
    return !!(canvas.getContext('webgl2') || canvas.getContext('webgl'))
  } catch {
    return false
  }
}

function createMapStyle(isDark: boolean): StyleSpecification {
  const tileUrl = isDark
    ? 'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'
    : 'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'
  return {
    version: 8,
    sources: {
      carto: {
        type: 'raster',
        tiles: [tileUrl],
        tileSize: 256,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      },
    },
    layers: [{ id: 'carto-tiles', type: 'raster', source: 'carto' }],
  }
}

// Lifted verbatim from the old page. Groups latency entries by device_pk, taking
// the best (min) avg latency across reachable IPs.
function parseLatencyJson(text: string): { results: DeviceLatency[] } | { error: string } {
  try {
    // Extract the JSON array — find [ at the start of a line to skip command echoes.
    const arrayMatch = text.match(/(?:^|\n)(\[[\s\S]*\])/)
    if (!arrayMatch) throw new Error('No JSON array found')
    const entries = JSON.parse(arrayMatch[1]) as LatencyEntry[]
    if (!Array.isArray(entries)) throw new Error('Expected a JSON array')

    const grouped = new Map<string, DeviceLatency>()
    for (const entry of entries) {
      const existing = grouped.get(entry.device_pk)
      if (!existing) {
        grouped.set(entry.device_pk, {
          device_pk: entry.device_pk,
          device_code: entry.device_code,
          avg_latency_ns: entry.reachable ? entry.avg_latency_ns : Number.MAX_SAFE_INTEGER,
          reachable: entry.reachable,
        })
      } else {
        grouped.set(entry.device_pk, {
          ...existing,
          avg_latency_ns: entry.reachable
            ? (existing.reachable
              ? Math.min(existing.avg_latency_ns, entry.avg_latency_ns)
              : entry.avg_latency_ns)
            : existing.avg_latency_ns,
          reachable: existing.reachable || entry.reachable,
        })
      }
    }

    const results = [...grouped.values()].sort((a, b) => {
      if (a.reachable && !b.reachable) return -1
      if (!a.reachable && b.reachable) return 1
      return a.avg_latency_ns - b.avg_latency_ns
    })

    return { results }
  } catch {
    return { error: 'Paste valid JSON output from: doublezero latency --json' }
  }
}

interface MetroSummary {
  code: string
  name: string
  latitude: number
  longitude: number
  devices: ShredDevice[]
  seatsFree: number
  minPrice: number
  hasRecommended: boolean
}

function summarizeByMetro(
  devices: ShredDevice[],
  metros: Metro[],
  latencyMap: Map<string, DeviceLatency> | undefined,
): MetroSummary[] {
  const metroByCode = new Map(metros.map(m => [m.code, m]))
  const grouped = new Map<string, ShredDevice[]>()
  for (const d of devices) {
    if (!metroByCode.has(d.metro_code)) continue
    const arr = grouped.get(d.metro_code) ?? []
    arr.push(d)
    grouped.set(d.metro_code, arr)
  }

  // Determine top-ranked reachable device (across all metros) for ⚡ badge.
  let topDeviceKey: string | null = null
  if (latencyMap) {
    let best: { key: string; ns: number } | null = null
    for (const d of devices) {
      const l = latencyMap.get(d.device_key)
      if (l?.reachable && (!best || l.avg_latency_ns < best.ns)) {
        best = { key: d.device_key, ns: l.avg_latency_ns }
      }
    }
    topDeviceKey = best?.key ?? null
  }

  const out: MetroSummary[] = []
  for (const [code, devs] of grouped) {
    const metro = metroByCode.get(code)!
    const seatsFree = devs.reduce((s, d) => s + Math.max(0, d.available_seats), 0)
    const minPrice = devs.reduce((p, d) => Math.min(p, d.total_price_dollars), Infinity)
    const hasRecommended = topDeviceKey ? devs.some(d => d.device_key === topDeviceKey) : false
    out.push({
      code,
      name: metro.name,
      latitude: metro.latitude,
      longitude: metro.longitude,
      devices: devs,
      seatsFree,
      minPrice: Number.isFinite(minPrice) ? minPrice : 0,
      hasRecommended,
    })
  }
  return out
}

interface MapLandingProps {
  devices: ShredDevice[]
  metros: Metro[]
  selectedMetro: string | null
  onSelectMetro: (metro: string | null) => void
  onSelectDevice: (d: ShredDevice) => void
  laneBanner?: React.ReactNode
}

export function MapLanding({
  devices,
  metros,
  selectedMetro,
  onSelectMetro,
  onSelectDevice,
  laneBanner,
}: MapLandingProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const mapStyle = useMemo(() => createMapStyle(isDark), [isDark])

  const [viewMode, setViewMode] = useState<'map' | 'list'>('map')
  const [latencyPaste, setLatencyPaste] = useState('')
  const [latencyResults, setLatencyResults] = useState<DeviceLatency[] | null>(null)
  const [latencyError, setLatencyError] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)
  const [latencyOpen, setLatencyOpen] = useState(false)

  // Compute WebGL support once per mount. If absent, fall back to the 2D
  // MapLibre map below — the picker still works without three.js.
  const webGLOk = useMemo(() => isWebGLAvailable(), [])

  const latencyMap = useMemo<Map<string, DeviceLatency> | undefined>(() => {
    if (!latencyResults) return undefined
    return new Map(latencyResults.map(r => [r.device_pk, r]))
  }, [latencyResults])

  const handleLatencyPaste = useCallback((text: string) => {
    setLatencyPaste(text)
    if (!text.trim()) {
      setLatencyResults(null)
      setLatencyError(null)
      return
    }
    const parsed = parseLatencyJson(text)
    if ('error' in parsed) {
      setLatencyResults(null)
      setLatencyError(parsed.error)
    } else {
      setLatencyResults(parsed.results)
      setLatencyError(null)
      // Auto-open the drawer for the metro of the top-ranked reachable device.
      const top = parsed.results.find(r => r.reachable && devices.some(d => d.device_key === r.device_pk))
      if (top) {
        const topDevice = devices.find(d => d.device_key === top.device_pk)
        if (topDevice) onSelectMetro(topDevice.metro_code)
      }
    }
  }, [devices, onSelectMetro])

  const handleCopyCommand = useCallback(() => {
    navigator.clipboard.writeText('doublezero latency --json | jq ".[0:5]"').then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }, [])

  const metroSummaries = useMemo(() => summarizeByMetro(devices, metros, latencyMap), [devices, metros, latencyMap])

  // Metros for the picker globe — one prominent pin per shred-eligible metro.
  const pickerMetros = useMemo<PickerGlobeMetro[]>(
    () => metroSummaries.map(m => ({
      code: m.code,
      name: m.name,
      latitude: m.latitude,
      longitude: m.longitude,
      seatsFree: m.seatsFree,
    })),
    [metroSummaries],
  )

  const openedMetro = selectedMetro
    ? metroSummaries.find(m => m.code === selectedMetro) ?? null
    : null

  const reachableCount = latencyResults?.filter(r => r.reachable).length ?? 0
  const topMatch = latencyResults?.find(r => r.reachable && devices.some(d => d.device_key === r.device_pk))
  const topMatchDevice = topMatch ? devices.find(d => d.device_key === topMatch.device_pk) : null

  return (
    <div className="space-y-4">
      {/* Subtitle + Map/List toggle */}
      <div className="flex flex-wrap items-end justify-between gap-3">
        <p className="text-sm text-muted-foreground">Pick a metro near your target server.</p>
        <div className="flex items-center gap-2">
          <ViewToggle value={viewMode} onChange={setViewMode} />
        </div>
      </div>

      {laneBanner}

      {viewMode === 'list' ? (
        <DevicePicker
          devices={devices}
          selected={null}
          onSelect={onSelectDevice}
          latencyMap={latencyMap}
        />
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-[1fr_360px] gap-4 items-start">
          {/* Map */}
          {webGLOk ? (
            <div className="border border-border rounded-lg overflow-hidden bg-card">
              <div className="relative w-full h-[60vh] min-h-[420px]">
                <Suspense fallback={<GlobeLoader />}>
                  <PickerGlobe
                    metros={pickerMetros}
                    selectedMetro={selectedMetro}
                    onSelectMetro={onSelectMetro}
                  />
                </Suspense>
              </div>
              <div className="flex flex-wrap items-center gap-4 px-4 py-2 text-xs text-muted-foreground border-t border-border">
                <div className="flex items-center gap-1.5">
                  <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: '#f97316' }} />
                  Seats free
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: '#6b7280' }} />
                  Full
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: '#3b82f6' }} />
                  Selected
                </div>
              </div>
            </div>
          ) : (
            <div className="border border-border rounded-lg overflow-hidden bg-card">
              <div className="relative w-full h-[60vh] min-h-[420px]">
                <MapGL
                  initialViewState={{ longitude: 10, latitude: 30, zoom: 1.4 }}
                  mapStyle={mapStyle}
                  style={{ width: '100%', height: '100%' }}
                  attributionControl={false}
                >
                  <NavigationControl position="top-right" showCompass={false} />
                  {metroSummaries.map(m => {
                    const isSelected = m.code === selectedMetro
                    const hasSeats = m.seatsFree > 0
                    return (
                      <Marker
                        key={m.code}
                        longitude={m.longitude}
                        latitude={m.latitude}
                        anchor="center"
                        onClick={e => {
                          e.originalEvent.stopPropagation()
                          onSelectMetro(m.code)
                        }}
                      >
                        <button
                          type="button"
                          title={`${m.code} · ${m.devices.length} device${m.devices.length !== 1 ? 's' : ''} · from $${m.minPrice}/ep · ${m.seatsFree} seat${m.seatsFree !== 1 ? 's' : ''} free`}
                          className="relative -translate-x-1/2 -translate-y-1/2 cursor-pointer"
                        >
                          <span
                            className={`block rounded-full border-2 transition-all ${
                              isSelected
                                ? 'h-4 w-4 border-foreground'
                                : 'h-3 w-3 border-background'
                            } ${hasSeats ? 'bg-accent-orange-100' : 'bg-muted'}`}
                            style={{
                              boxShadow: isSelected ? '0 0 0 3px rgba(0,0,0,0.15)' : '0 1px 2px rgba(0,0,0,0.25)',
                            }}
                          />
                          {m.hasRecommended && (
                            <Zap
                              className="absolute -top-1 -right-1 h-3 w-3 text-foreground"
                              fill="currentColor"
                            />
                          )}
                        </button>
                      </Marker>
                    )
                  })}
                </MapGL>
              </div>
              {/* Legend */}
              <div className="flex flex-wrap items-center gap-4 px-4 py-2 text-xs text-muted-foreground border-t border-border">
                <div className="flex items-center gap-1.5">
                  <span className="inline-block h-2.5 w-2.5 rounded-full bg-accent-orange-100" />
                  Seats free
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="inline-block h-2.5 w-2.5 rounded-full bg-muted border border-border" />
                  Full
                </div>
                {latencyMap && (
                  <div className="flex items-center gap-1.5">
                    <Zap className="h-3 w-3" fill="currentColor" />
                    Recommended (closest to your server)
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Drawer */}
          <aside className="sticky top-4 border border-border rounded-lg bg-card">
            {openedMetro ? (
              <div className="p-4 space-y-3">
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0">
                    <div className="text-base font-medium truncate">{openedMetro.name}</div>
                    <div className="text-xs text-muted-foreground">
                      {openedMetro.code} · {openedMetro.devices.length} device{openedMetro.devices.length !== 1 ? 's' : ''} · {openedMetro.seatsFree} seat{openedMetro.seatsFree !== 1 ? 's' : ''} free
                    </div>
                  </div>
                  <button
                    onClick={() => onSelectMetro(null)}
                    className="p-1 rounded hover:bg-muted text-muted-foreground hover:text-foreground transition-colors"
                    title="Close"
                  >
                    <X className="h-4 w-4" />
                  </button>
                </div>

                {/* Device rows */}
                <div className="space-y-1.5">
                  {openedMetro.devices
                    .slice()
                    .sort((a, b) => {
                      const la = latencyMap?.get(a.device_key)
                      const lb = latencyMap?.get(b.device_key)
                      const ar = la?.reachable ?? false
                      const br = lb?.reachable ?? false
                      if (ar && !br) return -1
                      if (!ar && br) return 1
                      if (ar && br) return la!.avg_latency_ns - lb!.avg_latency_ns
                      return b.available_seats - a.available_seats
                    })
                    .map(d => {
                      const latency = latencyMap?.get(d.device_key)
                      const hasSeats = d.available_seats > 0
                      return (
                        <div
                          key={d.device_key}
                          className={`flex items-center justify-between gap-2 px-3 py-2 rounded border border-border ${hasSeats ? 'bg-background hover:bg-muted/50 transition-colors' : 'opacity-50'}`}
                        >
                          <div className="min-w-0 flex-1">
                            <div className="text-sm font-mono font-medium truncate">{d.device_code}</div>
                            <div className="text-xs text-muted-foreground flex items-center gap-2">
                              {hasSeats ? (
                                <span>{d.available_seats} seat{d.available_seats !== 1 ? 's' : ''} free</span>
                              ) : (
                                <span className="text-red-500">Full</span>
                              )}
                              <span>·</span>
                              <span>${d.total_price_dollars}/ep</span>
                              {latency?.reachable && (
                                <>
                                  <span>·</span>
                                  <span className="tabular-nums">{(latency.avg_latency_ns / 1e6).toFixed(1)}ms</span>
                                </>
                              )}
                            </div>
                          </div>
                          <button
                            disabled={!hasSeats}
                            onClick={() => onSelectDevice(d)}
                            className="px-3 py-1.5 text-xs rounded bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                          >
                            Select →
                          </button>
                        </div>
                      )
                    })}
                </div>

                {/* Latency expander */}
                <LatencyExpander
                  open={latencyOpen}
                  setOpen={setLatencyOpen}
                  latencyPaste={latencyPaste}
                  onPaste={handleLatencyPaste}
                  copied={copied}
                  onCopyCommand={handleCopyCommand}
                  error={latencyError}
                  hasResults={!!latencyResults}
                  reachableCount={reachableCount}
                  topMatchDevice={topMatchDevice ?? null}
                  topMatchLatencyNs={topMatch?.avg_latency_ns ?? null}
                />
              </div>
            ) : (
              <div className="p-4 space-y-3">
                <div className="text-sm text-muted-foreground">
                  Click a pin on the map to see devices in that metro.
                </div>
                <LatencyExpander
                  open={latencyOpen}
                  setOpen={setLatencyOpen}
                  latencyPaste={latencyPaste}
                  onPaste={handleLatencyPaste}
                  copied={copied}
                  onCopyCommand={handleCopyCommand}
                  error={latencyError}
                  hasResults={!!latencyResults}
                  reachableCount={reachableCount}
                  topMatchDevice={topMatchDevice ?? null}
                  topMatchLatencyNs={topMatch?.avg_latency_ns ?? null}
                />
              </div>
            )}
          </aside>
        </div>
      )}
    </div>
  )
}

function ViewToggle({ value, onChange }: { value: 'map' | 'list'; onChange: (v: 'map' | 'list') => void }) {
  return (
    <div className="inline-flex rounded-lg border border-border overflow-hidden text-sm">
      <button
        onClick={() => onChange('map')}
        className={`px-3 py-1.5 transition-colors ${value === 'map' ? 'bg-foreground text-background' : 'bg-background text-muted-foreground hover:bg-muted'}`}
      >
        Map
      </button>
      <button
        onClick={() => onChange('list')}
        className={`px-3 py-1.5 transition-colors border-l border-border ${value === 'list' ? 'bg-foreground text-background' : 'bg-background text-muted-foreground hover:bg-muted'}`}
      >
        List
      </button>
    </div>
  )
}

interface LatencyExpanderProps {
  open: boolean
  setOpen: (v: boolean) => void
  latencyPaste: string
  onPaste: (v: string) => void
  copied: boolean
  onCopyCommand: () => void
  error: string | null
  hasResults: boolean
  reachableCount: number
  topMatchDevice: ShredDevice | null
  topMatchLatencyNs: number | null
}

function LatencyExpander({
  open,
  setOpen,
  latencyPaste,
  onPaste,
  copied,
  onCopyCommand,
  error,
  hasResults,
  reachableCount,
  topMatchDevice,
  topMatchLatencyNs,
}: LatencyExpanderProps) {
  return (
    <div className="border-t border-border pt-3">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 w-full text-left text-xs font-medium text-muted-foreground hover:text-foreground transition-colors uppercase tracking-wider"
      >
        {open ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
        Don't know which device? Run a latency check.
      </button>
      {open && (
        <div className="mt-3 space-y-2">
          <p className="text-xs text-muted-foreground">
            Run this on the server that will receive shreds — not on this machine.
          </p>
          <div className="flex items-center gap-2">
            <code className="flex-1 px-2.5 py-1.5 text-xs bg-muted rounded font-mono border border-border truncate">
              {'doublezero latency --json | jq ".[0:5]"'}
            </code>
            <button
              onClick={onCopyCommand}
              className="flex items-center gap-1 px-2.5 py-1.5 text-xs border border-border rounded hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
              title="Copy command"
            >
              {copied ? <Check className="h-3 w-3 text-green-500" /> : <Copy className="h-3 w-3" />}
              {copied ? 'Copied' : 'Copy'}
            </button>
          </div>
          <textarea
            value={latencyPaste}
            onChange={e => onPaste(e.target.value)}
            placeholder="Paste JSON output here..."
            rows={5}
            className="w-full px-2 py-1.5 text-xs border border-border rounded bg-background focus:outline-none focus:ring-2 focus:ring-primary/50 font-mono resize-none"
          />
          {error && <p className="text-xs text-red-500">{error}</p>}
          {hasResults && !error && topMatchDevice && topMatchLatencyNs !== null && (
            <p className="text-xs text-green-600 dark:text-green-400 flex items-center gap-1">
              <Check className="h-3 w-3 shrink-0" />
              Found {reachableCount} reachable device{reachableCount !== 1 ? 's' : ''} — top match:{' '}
              <span className="font-mono font-medium">{topMatchDevice.device_code}</span>
              {' '}({(topMatchLatencyNs / 1e6).toFixed(1)}ms)
            </p>
          )}
        </div>
      )}
    </div>
  )
}

function GlobeLoader() {
  return (
    <div className="absolute inset-0 flex items-center justify-center bg-background">
      <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
    </div>
  )
}
