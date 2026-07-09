import { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import {
  Loader2,
  Route,
  AlertCircle,
  ArrowRight,
  ArrowDown,
  ExternalLink,
  RotateCcw,
  ChevronDown,
  ChevronRight,
  Map as MapIcon,
  Copy,
  Check,
} from 'lucide-react'
import {
  fetchTopology,
  fetchISISTopology,
  fetchISISPaths,
  fetchMetroDevicePaths,
} from '@/lib/api'
import type {
  SinglePath,
  PathService,
  MetroDevicePairPath,
} from '@/lib/api'
import {
  buildLocationOptions,
  parseEndpointKind,
  orderPairsBestFirst,
  resolveMetroPair,
  filterPairsForDevice,
  type LocationOption,
} from '@/lib/path-calculator'
import { ServiceToggle, LocationSearch } from '@/components/topology'

// Path colors matching the graph view
const PATH_COLORS = [
  '#22c55e', // green - primary/shortest
  '#3b82f6', // blue - alternate 1
  '#a855f7', // purple - alternate 2
  '#f97316', // orange - alternate 3
  '#06b6d4', // cyan - alternate 4
]

function PathCard({
  path,
  index,
  label,
  badge,
  isSelected,
  onSelect,
}: {
  path: SinglePath
  index: number
  label: string
  badge?: string
  isSelected: boolean
  onSelect: () => void
}) {
  const [copied, setCopied] = useState(false)

  const copyPath = () => {
    const pathText = path.path.map((h) => h.deviceCode).join(' → ')
    navigator.clipboard.writeText(pathText)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div
      className={`border rounded-lg p-4 cursor-pointer transition-all ${
        isSelected
          ? 'border-primary bg-primary/5 ring-2 ring-primary/20'
          : 'border-border hover:border-primary/50'
      }`}
      onClick={onSelect}
    >
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div
            className="w-3 h-3 rounded-full"
            style={{ backgroundColor: PATH_COLORS[index % PATH_COLORS.length] }}
          />
          <span className="font-medium">{label}</span>
          {badge && (
            <span className="text-xs bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400 px-2 py-0.5 rounded">
              {badge}
            </span>
          )}
        </div>
        <button
          onClick={(e) => {
            e.stopPropagation()
            copyPath()
          }}
          className="p-1.5 hover:bg-muted rounded-md"
          title="Copy path"
        >
          {copied ? (
            <Check className="h-4 w-4 text-green-500" />
          ) : (
            <Copy className="h-4 w-4 text-muted-foreground" />
          )}
        </button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4 text-sm">
        <div>
          <span className="text-muted-foreground">Hops:</span>{' '}
          <span className="font-medium">{path.hopCount}</span>
        </div>
        <div>
          <span className="text-muted-foreground">ISIS Metric:</span>{' '}
          <span className="font-medium">{(path.totalMetric / 1000).toFixed(2)}ms</span>
        </div>
        {path.measuredLatencyMs !== undefined && path.measuredLatencyMs > 0 && (
          <>
            <div>
              <span className="text-muted-foreground">Measured:</span>{' '}
              <span className="font-medium text-primary">
                {path.measuredLatencyMs.toFixed(2)}ms
              </span>
            </div>
            {path.totalSamples !== undefined && (
              <div>
                <span className="text-muted-foreground">Samples:</span>{' '}
                <span className="font-medium">{path.totalSamples.toLocaleString()}</span>
              </div>
            )}
          </>
        )}
      </div>

      <div className="space-y-3">
        {path.path.map((hop, i) => (
          <div key={hop.devicePK} className="text-sm">
            <div className="flex items-center gap-2">
              <span className="w-5 shrink-0 text-muted-foreground">{i + 1}.</span>
              <Link
                to={`/dz/devices/${hop.devicePK}`}
                onClick={(e) => e.stopPropagation()}
                className="font-mono hover:text-primary flex items-center gap-1 min-w-0 break-all"
              >
                {hop.deviceCode}
                <ExternalLink className="h-3 w-3 shrink-0 opacity-0 hover:opacity-100" />
              </Link>
              <div className="hidden sm:flex items-center gap-2 ml-auto shrink-0">
                {hop.edgeMeasuredMs !== undefined && hop.edgeMeasuredMs > 0 && (
                  <span
                    className="text-primary text-xs"
                    title={`Measured RTT: ${hop.edgeMeasuredMs.toFixed(2)}ms (${hop.edgeSampleCount?.toLocaleString() ?? 0} samples)`}
                  >
                    {hop.edgeMeasuredMs.toFixed(1)}ms meas.
                  </span>
                )}
                {hop.edgeLossPct !== undefined && hop.edgeLossPct > 0.1 && (
                  <span
                    className={`text-xs ${hop.edgeLossPct > 1 ? 'text-red-500' : 'text-yellow-500'}`}
                    title={`Packet loss: ${hop.edgeLossPct.toFixed(2)}%`}
                  >
                    {hop.edgeLossPct.toFixed(1)}% loss
                  </span>
                )}
                {hop.edgeMetric !== undefined && hop.edgeMetric > 0 && (
                  <span
                    className="text-muted-foreground text-xs"
                    title="ISIS metric (configured on router)"
                  >
                    {(hop.edgeMetric / 1000).toFixed(1)}ms ISIS
                  </span>
                )}
              </div>
            </div>
            {(hop.edgeMeasuredMs !== undefined && hop.edgeMeasuredMs > 0) ||
            (hop.edgeLossPct !== undefined && hop.edgeLossPct > 0.1) ||
            (hop.edgeMetric !== undefined && hop.edgeMetric > 0) ? (
              <div className="sm:hidden flex items-center gap-2 pl-7 mt-0.5 text-xs">
                {hop.edgeMeasuredMs !== undefined && hop.edgeMeasuredMs > 0 && (
                  <span
                    className="text-primary"
                    title={`Measured RTT: ${hop.edgeMeasuredMs.toFixed(2)}ms`}
                  >
                    {hop.edgeMeasuredMs.toFixed(1)}ms meas.
                  </span>
                )}
                {hop.edgeLossPct !== undefined && hop.edgeLossPct > 0.1 && (
                  <span className={hop.edgeLossPct > 1 ? 'text-red-500' : 'text-yellow-500'}>
                    {hop.edgeLossPct.toFixed(1)}% loss
                  </span>
                )}
                {hop.edgeMetric !== undefined && hop.edgeMetric > 0 && (
                  <span className="text-muted-foreground">
                    {(hop.edgeMetric / 1000).toFixed(1)}ms ISIS
                  </span>
                )}
              </div>
            ) : null}
          </div>
        ))}
      </div>
    </div>
  )
}

export function PathCalculatorPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [source, setSource] = useState<LocationOption | null>(null)
  const [target, setTarget] = useState<LocationOption | null>(null)
  const [selectedPathIndex, setSelectedPathIndex] = useState(0)
  const [showAllPairs, setShowAllPairs] = useState(false)
  const [initializedFromUrl, setInitializedFromUrl] = useState(false)
  const [service, setService] = useState<PathService>(
    (searchParams.get('service') as PathService) || 'unicast',
  )

  const { data: isis, isLoading: isisLoading, isError: isisError } = useQuery({
    queryKey: ['isis-topology'],
    queryFn: fetchISISTopology,
    staleTime: 60000,
  })
  const {
    data: topology,
    isLoading: topoLoading,
    isError: topoError,
  } = useQuery({
    queryKey: ['topology'],
    queryFn: fetchTopology,
    staleTime: 60000,
  })

  // Merge metros + devices into one option list. Device set comes from ISIS
  // (as before); metro linkage (metroPK) is joined from the topology payload.
  const options: LocationOption[] = useMemo(() => {
    const metroPKByDevice = new Map((topology?.devices ?? []).map((d) => [d.pk, d.metro_pk]))
    const metros = (topology?.metros ?? []).map((m) => ({ pk: m.pk, code: m.code, name: m.name }))
    const devices = (isis?.nodes ?? []).map((n) => ({
      pk: n.data.id,
      code: n.data.label,
      status: n.data.status,
      deviceType: n.data.deviceType,
      metroPK: metroPKByDevice.get(n.data.id) ?? n.data.metroPK,
    }))
    return buildLocationOptions(metros, devices)
  }, [isis, topology])

  useEffect(() => {
    if (initializedFromUrl || isisLoading || topoLoading) return
    // Don't latch init until options actually arrived — a failed/empty fetch
    // must not silently drop the URL from/to and block a later recovery.
    if (options.length === 0) return
    const findOpt = (raw: string | null, kind: 'metro' | 'device') =>
      raw ? options.find((o) => o.kind === kind && (o.pk === raw || o.code === raw)) ?? null : null
    const s = findOpt(searchParams.get('from'), parseEndpointKind(searchParams.get('fromType')))
    const t = findOpt(searchParams.get('to'), parseEndpointKind(searchParams.get('toType')))
    if (s) setSource(s)
    if (t) setTarget(t)
    setInitializedFromUrl(true)
  }, [options, searchParams, initializedFromUrl, isisLoading, topoLoading])

  const writeEndpoint = (
    which: 'from' | 'to',
    opt: LocationOption | null,
    params: URLSearchParams,
  ) => {
    const typeKey = which === 'from' ? 'fromType' : 'toType'
    if (opt) {
      params.set(which, opt.pk)
      if (opt.kind === 'metro') params.set(typeKey, 'metro')
      else params.delete(typeKey) // device is the default → keep old links clean
    } else {
      params.delete(which)
      params.delete(typeKey)
    }
  }

  const updateSource = (opt: LocationOption | null) => {
    setSource(opt)
    setSelectedPathIndex(0)
    setShowAllPairs(false)
    const p = new URLSearchParams(searchParams)
    writeEndpoint('from', opt, p)
    setSearchParams(p, { replace: true })
  }
  const updateTarget = (opt: LocationOption | null) => {
    setTarget(opt)
    setSelectedPathIndex(0)
    setShowAllPairs(false)
    const p = new URLSearchParams(searchParams)
    writeEndpoint('to', opt, p)
    setSearchParams(p, { replace: true })
  }
  const resetSelection = () => {
    setSource(null)
    setTarget(null)
    setSelectedPathIndex(0)
    setShowAllPairs(false)
    setSearchParams({}, { replace: true })
  }
  const updateService = (s: PathService) => {
    setService(s)
    setSelectedPathIndex(0)
    const p = new URLSearchParams(searchParams)
    if (s !== 'unicast') p.set('service', s)
    else p.delete('service')
    setSearchParams(p, { replace: true })
  }

  const bothSelected = !!source && !!target
  const anyMetro = !!source && !!target && (source.kind === 'metro' || target.kind === 'metro')

  const {
    data: result,
    isLoading: resultLoading,
    error: resultError,
  } = useQuery({
    queryKey: ['path-calc', source?.kind, source?.pk, target?.kind, target?.pk, service],
    enabled: bothSelected,
    queryFn: async (): Promise<{
      paths: SinglePath[]
      pairs: MetroDevicePairPath[] | null
      error?: string
    }> => {
      const s = source!
      const t = target!
      if (!anyMetro) {
        const res = await fetchISISPaths(s.pk, t.pk, 5, service)
        return { paths: res.paths ?? [], pairs: null, error: res.error }
      }
      const { fromMetro, toMetro, error } = resolveMetroPair(s, t)
      if (!fromMetro || !toMetro || error) {
        return { paths: [], pairs: [], error }
      }
      const res = await fetchMetroDevicePaths(fromMetro, toMetro, service)
      const filtered = filterPairsForDevice(res.devicePairs ?? [], {
        sourceDevicePK: s.kind === 'device' ? s.pk : undefined,
        targetDevicePK: t.kind === 'device' ? t.pk : undefined,
      })
      // Best pair first (and the whole list ordered by the same criterion) so
      // card ordering matches the "Best" badge and "Pair N" reflects rank.
      const ordered = orderPairsBestFirst(filtered)
      return { paths: ordered.map((p) => p.bestPath), pairs: ordered, error: res.error }
    },
  })

  const paths = result?.paths ?? []
  const pairs = result?.pairs ?? null

  // Clamp the selected path when a new result set arrives (e.g. fewer paths than before).
  useEffect(() => {
    const len = result?.paths?.length ?? 0
    setSelectedPathIndex((i) => (i >= len && len > 0 ? 0 : i))
  }, [result])

  // Device pks used for the "View in graph" link (resolve metros via the best/selected pair).
  const graphSourcePK = anyMetro ? pairs?.[selectedPathIndex]?.sourceDevicePK : source?.pk
  const graphTargetPK = anyMetro ? pairs?.[selectedPathIndex]?.targetDevicePK : target?.pk

  if (isisLoading || topoLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (isisError || topoError) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="flex items-center gap-2 text-red-500">
          <AlertCircle className="h-6 w-6" />
          <span>Failed to load network topology. Please try again.</span>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto px-4 sm:px-8 py-8">
        <div className="flex items-center justify-between gap-3 mb-2">
          <div className="flex items-center gap-3">
            <Route className="h-6 w-6 text-muted-foreground" />
            <h1 className="text-2xl font-medium">Path Calculator</h1>
          </div>
          <Link
            to="/topology/map"
            className="text-sm text-primary hover:underline flex items-center gap-1"
          >
            <MapIcon className="h-4 w-4" />
            Open in Map
          </Link>
        </div>

        <p className="text-muted-foreground mb-6">
          Path Calculator shows the DoubleZero network route between two locations. Search by
          city or metro, or by a specific device.
        </p>

        <ServiceToggle
          value={service}
          onChange={updateService}
          size="md"
          label="Traffic type"
          showDescription
          className="mb-6"
        />

        <div className="bg-card border border-border rounded-lg p-6 mb-6">
          <div className="flex items-end gap-4 flex-col sm:flex-row">
            <LocationSearch
              label="Source"
              placeholder="Search city, metro, or device..."
              value={source}
              onChange={updateSource}
              options={options}
              excludePK={target?.pk}
            />
            <div className="self-center sm:self-auto sm:pb-2">
              <ArrowRight className="hidden sm:block h-5 w-5 text-muted-foreground" />
              <ArrowDown className="block sm:hidden h-5 w-5 text-muted-foreground" />
            </div>
            <LocationSearch
              label="Destination"
              placeholder="Search city, metro, or device..."
              value={target}
              onChange={updateTarget}
              options={options}
              excludePK={source?.pk}
            />
            {(source || target) && (
              <button
                onClick={resetSelection}
                className="pb-2 p-2 hover:bg-muted rounded-md text-muted-foreground hover:text-foreground transition-colors"
                title="Reset selection"
              >
                <RotateCcw className="h-5 w-5" />
              </button>
            )}
          </div>

          {bothSelected && graphSourcePK && graphTargetPK && (
            <div className="mt-4 pt-4 border-t border-border flex items-center justify-between md:justify-start">
              <Link
                to={`/topology/graph?path_source=${graphSourcePK}&path_target=${graphTargetPK}${service !== 'unicast' ? `&path_service=${service}` : ''}`}
                className="text-sm text-primary hover:underline flex items-center gap-1"
              >
                View in graph
                <ExternalLink className="h-3 w-3" />
              </Link>
            </div>
          )}
        </div>

        {resultLoading && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground mr-2" />
            <span className="text-muted-foreground">Finding paths...</span>
          </div>
        )}
        {resultError && (
          <div className="flex items-center justify-center py-12">
            <AlertCircle className="h-6 w-6 text-red-500 mr-2" />
            <span className="text-red-500">{(resultError as Error).message}</span>
          </div>
        )}
        {result?.error && (
          <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
            <div className="flex items-center gap-2 text-red-700 dark:text-red-400">
              <AlertCircle className="h-5 w-5" />
              <span>{result.error}</span>
            </div>
          </div>
        )}

        {paths.length > 0 && (
          <div>
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-medium">
                {anyMetro
                  ? 'Best path'
                  : `${paths.length} ${paths.length === 1 ? 'Path' : 'Paths'} Found`}
              </h2>
              {anyMetro && pairs && pairs.length > 1 && (
                <button
                  onClick={() => {
                    setShowAllPairs((v) => !v)
                    setSelectedPathIndex(0)
                  }}
                  className="text-sm text-primary hover:underline flex items-center gap-1"
                >
                  {showAllPairs ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                  {showAllPairs ? 'Hide' : `Show all ${pairs.length} device pairs`}
                </button>
              )}
            </div>

            {anyMetro && pairs ? (
              <div className="grid gap-4">
                {(showAllPairs ? pairs : pairs.slice(0, 1)).map((pair, index) => (
                  <div key={`${pair.sourceDevicePK}-${pair.targetDevicePK}`}>
                    <div className="text-xs text-muted-foreground mb-1 font-mono">
                      {pair.sourceDeviceCode} → {pair.targetDeviceCode}
                    </div>
                    <PathCard
                      path={pair.bestPath}
                      index={index}
                      label={`Pair ${index + 1}`}
                      badge={index === 0 ? 'Best' : undefined}
                      isSelected={index === selectedPathIndex}
                      onSelect={() => setSelectedPathIndex(index)}
                    />
                  </div>
                ))}
              </div>
            ) : (
              <div className="grid gap-4">
                {paths.map((path, index) => (
                  <PathCard
                    key={index}
                    path={path}
                    index={index}
                    label={`Path ${index + 1}`}
                    badge={index === 0 ? 'Shortest' : undefined}
                    isSelected={index === selectedPathIndex}
                    onSelect={() => setSelectedPathIndex(index)}
                  />
                ))}
              </div>
            )}
          </div>
        )}

        {bothSelected && !resultLoading && !result?.error && paths.length === 0 && (
          <div className="text-center py-12 text-muted-foreground">
            <Route className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>No paths found between these locations.</p>
            <p className="text-sm mt-2">They may not be connected in the ISIS topology.</p>
          </div>
        )}
      </div>
    </div>
  )
}
