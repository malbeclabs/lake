import { useMemo, useState, useCallback, useEffect } from 'react'
import MapGL, { Source, Layer } from 'react-map-gl/maplibre'
import type { MapLayerMouseEvent } from 'react-map-gl/maplibre'
import type { StyleSpecification } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useQuery } from '@tanstack/react-query'
import { useTheme } from '@/hooks/use-theme'
import { Loader2, AlertCircle, ExternalLink, ChevronUp, ChevronDown } from 'lucide-react'
import { ResponsiveContainer, PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts'
import { fetchGeoValidators, fetchMetros, type GeoValidatorsResponse, type GeoValidatorItem } from '@/lib/api'

function createMapStyle(isDark: boolean): StyleSpecification {
  const tileUrl = isDark
    ? 'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'
    : 'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'
  return {
    version: 8,
    sources: {
      carto: { type: 'raster', tiles: [tileUrl], tileSize: 256,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>' },
    },
    layers: [{ id: 'carto-tiles', type: 'raster', source: 'carto', minzoom: 0, maxzoom: 22 }],
  }
}

function formatPct(v: number): string {
  return v < 0.1 ? '<0.1%' : `${v.toFixed(1)}%`
}

function formatSol(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
  if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`
  return v.toFixed(0)
}

// Deterministic sub-degree offset to visually separate validators at the same metro
function hashOffset(pubkey: string): [number, number] {
  let h = 0
  for (let i = 0; i < 8; i++) h = (h * 31 + pubkey.charCodeAt(i)) | 0
  const angle = ((h & 0xffff) / 0xffff) * 2 * Math.PI
  const dist = 0.3 + ((h >>> 16) & 0xff) / 255 * 0.4
  return [Math.cos(angle) * dist, Math.sin(angle) * dist]
}

const TIER_COLORS: Record<string, string> = {
  super: '#22c55e',
  high: '#3b82f6',
  mid: '#f59e0b',
}

const GHOST_ROWS = [
  { name: 'Validator Alpha', metro: 'FRA', dc: 'Equinix FR5', stake: 1_234_567, pct: 0.31, comm: 5, dz: true },
  { name: 'StakeHouse', metro: 'AMS', dc: 'Interxion AMS7', stake: 987_654, pct: 0.25, comm: 7, dz: false },
  { name: 'SolanaBeach', metro: 'TYO', dc: 'Equinix TY3', stake: 876_543, pct: 0.22, comm: 8, dz: true },
  { name: 'BlockDaemon', metro: 'SIN', dc: 'Equinix SG1', stake: 765_432, pct: 0.19, comm: 6, dz: false },
  { name: 'Figment', metro: 'ORD', dc: 'CoreSite CH1', stake: 654_321, pct: 0.16, comm: 5, dz: true },
  { name: 'P2P Validator', metro: 'WAW', dc: 'Equinix WA1', stake: 543_210, pct: 0.14, comm: 10, dz: false },
]

type SortKey = 'stake_sol' | 'name' | 'metro_code' | 'datacenter' | 'commission' | 'is_dz'

function getSortValue(v: GeoValidatorItem, key: SortKey): string | number | boolean {
  switch (key) {
    case 'stake_sol': return v.stake_sol
    case 'name': return v.name || v.vote_pubkey
    case 'metro_code': return v.metro_code
    case 'datacenter': return v.datacenter
    case 'commission': return v.commission
    case 'is_dz': return v.is_dz
  }
}

function FreshnessIndicator({ dataUpdatedAt }: { dataUpdatedAt: number }) {
  const [now, setNow] = useState(Date.now())

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 30_000)
    return () => clearInterval(id)
  }, [])

  const diffMin = Math.floor((now - dataUpdatedAt) / 60_000)
  const color = diffMin < 5 ? 'bg-green-500' : diffMin < 15 ? 'bg-amber-500' : 'bg-muted-foreground'
  const label = diffMin < 1 ? 'Updated just now' : `Updated ${diffMin}m ago`

  return (
    <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground" title="Data refreshes every 60 seconds from DZ infrastructure">
      <span className={`inline-block w-1.5 h-1.5 rounded-full ${color}`} />
      {label}
    </div>
  )
}

function ValidatorScatterMap({ data, validators, metroCoords }: {
  data: GeoValidatorsResponse
  validators: GeoValidatorItem[]
  metroCoords: Map<string, { lat: number; lng: number; name: string }>
}) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const mapStyle = useMemo(() => createMapStyle(isDark), [isDark])
  const [hoverInfo, setHoverInfo] = useState<{
    x: number; y: number; name: string; metro: string
    stake: number; datacenter: string; commission: number; isDz: boolean
  } | null>(null)

  const clusterGeoJSON = useMemo(() => ({
    type: 'FeatureCollection' as const,
    features: data.metro_breakdown
      .map((m) => {
        const coords = metroCoords.get(m.metro_code)
        if (!coords) return null
        return {
          type: 'Feature' as const,
          properties: {
            radius: Math.max(6, Math.sqrt(m.stake_pct) * 5),
          },
          geometry: { type: 'Point' as const, coordinates: [coords.lng, coords.lat] },
        }
      })
      .filter((f): f is NonNullable<typeof f> => f !== null),
  }), [data.metro_breakdown, metroCoords])

  const validatorGeoJSON = useMemo(() => ({
    type: 'FeatureCollection' as const,
    features: validators
      .filter(v => v.dzdp_lat !== 0 || v.dzdp_lng !== 0)
      .map((v) => {
        const [dx, dy] = hashOffset(v.vote_pubkey)
        return {
          type: 'Feature' as const,
          properties: {
            name: v.name || `${v.vote_pubkey.slice(0, 4)}...${v.vote_pubkey.slice(-4)}`,
            metro: v.metro_code,
            stake: v.stake_sol,
            datacenter: v.datacenter,
            commission: v.commission,
            is_dz: v.is_dz,
            radius: Math.max(4, Math.sqrt(v.stake_pct) * 8),
            color: v.is_dz ? '#22c55e' : '#3b82f6',
            stroke_opacity: v.is_dz ? 0.9 : 0.5,
          },
          geometry: { type: 'Point' as const, coordinates: [v.dzdp_lng + dx, v.dzdp_lat + dy] },
        }
      }),
  }), [validators])

  const onHover = useCallback((event: MapLayerMouseEvent) => {
    const feature = event.features?.[0]
    if (feature) {
      setHoverInfo({
        x: event.point.x, y: event.point.y,
        name: String(feature.properties?.name ?? ''),
        metro: String(feature.properties?.metro ?? ''),
        stake: Number(feature.properties?.stake ?? 0),
        datacenter: String(feature.properties?.datacenter ?? ''),
        commission: Number(feature.properties?.commission ?? 0),
        isDz: feature.properties?.is_dz === true || feature.properties?.is_dz === 'true',
      })
    } else {
      setHoverInfo(null)
    }
  }, [])

  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-5 py-3 border-b border-border">
        <h3 className="text-sm font-medium">Validator Scatter Map</h3>
      </div>
      <div className="relative h-[350px]">
        <MapGL
          initialViewState={{ longitude: 0, latitude: 20, zoom: 1.5 }}
          style={{ width: '100%', height: '100%' }}
          mapStyle={mapStyle}
          interactiveLayerIds={['validator-dots']}
          onMouseMove={onHover}
          onMouseLeave={() => setHoverInfo(null)}
        >
          <Source id="metro-clusters" type="geojson" data={clusterGeoJSON}>
            <Layer id="metro-cluster-circles" type="circle" paint={{
              'circle-radius': ['get', 'radius'],
              'circle-color': '#3b82f6',
              'circle-opacity': 0.15,
              'circle-stroke-width': 0,
            }} />
          </Source>
          <Source id="validator-points" type="geojson" data={validatorGeoJSON}>
            <Layer id="validator-dots" type="circle" paint={{
              'circle-radius': ['get', 'radius'],
              'circle-color': ['get', 'color'],
              'circle-opacity': 0.75,
              'circle-stroke-width': 1.5,
              'circle-stroke-color': isDark ? 'rgba(255,255,255,0.3)' : 'rgba(0,0,0,0.15)',
              'circle-stroke-opacity': ['get', 'stroke_opacity'],
            }} />
          </Source>
        </MapGL>
        {validatorGeoJSON.features.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <div className="bg-card/90 backdrop-blur-sm border border-border rounded-lg px-6 py-4 text-center">
              <div className="text-sm text-muted-foreground">No validator location data available</div>
            </div>
          </div>
        )}
        {hoverInfo && (
          <div className="absolute z-10 pointer-events-none bg-popover border border-border rounded-lg px-3 py-2 shadow-lg text-sm"
            style={{ left: hoverInfo.x + 12, top: hoverInfo.y - 12 }}>
            <div className="font-medium">{hoverInfo.name}</div>
            <div className="text-muted-foreground">Metro: {hoverInfo.metro}</div>
            <div className="text-muted-foreground">Stake: {formatSol(hoverInfo.stake)} SOL</div>
            <div className="text-muted-foreground">DC: {hoverInfo.datacenter}</div>
            <div className="text-muted-foreground">Commission: {hoverInfo.commission}%</div>
            <div className="text-muted-foreground">DZ: {hoverInfo.isDz ? 'Yes' : 'No'}</div>
          </div>
        )}
      </div>
    </div>
  )
}

function StakeTierBreakdown({ data, metroCoords, donutMode, setDonutMode }: {
  data: GeoValidatorsResponse
  metroCoords: Map<string, { lat: number; lng: number; name: string }>
  donutMode: 'count' | 'stake'
  setDonutMode: (mode: 'count' | 'stake') => void
}) {
  const tierData = useMemo(() =>
    data.tier_distribution.map(t => ({ ...t, fill: TIER_COLORS[t.tier] || '#6b7280' })),
    [data.tier_distribution],
  )

  const topMetros = useMemo(() =>
    [...data.metro_breakdown]
      .sort((a, b) => b.stake_sol - a.stake_sol)
      .map(m => ({
        ...m,
        name: metroCoords.get(m.metro_code)?.name || m.metro_code,
      })),
    [data.metro_breakdown, metroCoords],
  )

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="px-5 py-3 border-b border-border flex items-center justify-between">
          <h3 className="text-sm font-medium">Stake Tier Distribution</h3>
          <div className="flex rounded-md border border-border overflow-hidden">
            {(['count', 'stake'] as const).map(mode => (
              <button key={mode} onClick={() => setDonutMode(mode)}
                className={`px-2.5 py-1 text-[11px] ${donutMode === mode ? 'bg-background text-foreground' : 'bg-card text-muted-foreground hover:text-foreground'}`}>
                {mode === 'count' ? 'Count' : 'Stake %'}
              </button>
            ))}
          </div>
        </div>
        <div className="px-5 py-4 flex items-center justify-center">
          {tierData.length > 0 ? (
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie data={tierData} dataKey={donutMode === 'count' ? 'validators' : 'stake_pct'}
                  nameKey="tier" cx="50%" cy="50%" innerRadius={50} outerRadius={80} paddingAngle={2}>
                  {tierData.map((entry, i) => <Cell key={i} fill={TIER_COLORS[entry.tier] || '#6b7280'} />)}
                </Pie>
                <Tooltip formatter={(value) => [donutMode === 'count' ? `${Number(value)} validators` : `${Number(value).toFixed(1)}%`, '']} />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="text-sm text-muted-foreground py-8">No tier data available</div>
          )}
        </div>
        {tierData.length > 0 && (
          <div className="px-5 pb-4 flex items-center justify-center gap-4">
            {tierData.map(t => (
              <div key={t.tier} className="flex items-center gap-1.5 text-[11px]">
                <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: TIER_COLORS[t.tier] || '#6b7280' }} />
                <span className="capitalize">{t.tier}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="rounded-lg border border-border bg-card overflow-hidden flex flex-col max-h-[500px]">
        <div className="px-5 py-3 border-b border-border flex-shrink-0">
          <h3 className="text-sm font-medium">Metros by Stake</h3>
        </div>
        <div className="px-5 py-4 overflow-y-auto">
          {topMetros.length > 0 ? (
            <ResponsiveContainer width="100%" height={topMetros.length * 28 + 20}>
              <BarChart data={topMetros} layout="vertical" margin={{ top: 0, right: 40, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
                <XAxis type="number" tickLine={false} axisLine={false} tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${v}%`} />
                <YAxis dataKey="metro_code" type="category" tickLine={false} axisLine={false} tick={{ fontSize: 11 }} width={50} />
                <Tooltip cursor={{ fill: 'var(--muted)', opacity: 0.4 }} formatter={(value) => [`${Number(value).toFixed(1)}%`, 'Stake']} />
                <Bar dataKey="stake_pct" radius={[0, 3, 3, 0]} fill="#3b82f6" />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="text-sm text-muted-foreground py-8 text-center">No metro data available</div>
          )}
        </div>
      </div>
    </div>
  )
}

export function DzdpValidatorView() {
  const [metro, setMetro] = useState('')
  const [dzFilter, setDzFilter] = useState<'on' | 'off' | undefined>(undefined)
  const [tierFilter, setTierFilter] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('stake_sol')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [donutMode, setDonutMode] = useState<'count' | 'stake'>('count')

  const { data, isLoading, error, dataUpdatedAt } = useQuery({
    queryKey: ['geo-validators', metro || undefined, dzFilter],
    queryFn: () => fetchGeoValidators(metro || undefined, dzFilter),
    refetchInterval: 60_000,
  })

  const { data: metrosData } = useQuery({
    queryKey: ['metros-for-validators'],
    queryFn: () => fetchMetros(500),
  })

  const metroCoords = useMemo(() => {
    const map = new Map<string, { lat: number; lng: number; name: string }>()
    if (metrosData?.items) {
      for (const m of metrosData.items) {
        map.set(m.code, { lat: m.latitude, lng: m.longitude, name: m.name })
      }
    }
    return map
  }, [metrosData])

  // Tier filtering is client-side because the API returns tier_distribution aggregates
  // but not a per-validator tier field. We recompute tiers using the same cumulative-stake
  // logic the server uses (top 33% = super, 33-66% = high, bottom 34% = mid).
  // metro/dz filters are server-side via query params.
  const filtered = useMemo(() => {
    if (!data) return []
    let list = data.validators
    if (tierFilter) {
      const sorted = [...data.validators].sort((a, b) => b.stake_sol - a.stake_sol)
      const totalStake = data.total_stake_sol
      let cumStake = 0
      const tierMap = new Map<string, string>()
      for (const v of sorted) {
        cumStake += v.stake_sol
        const pct = (cumStake / totalStake) * 100
        if (pct <= 33) tierMap.set(v.vote_pubkey, 'super')
        else if (pct <= 66) tierMap.set(v.vote_pubkey, 'high')
        else tierMap.set(v.vote_pubkey, 'mid')
      }
      list = list.filter(v => tierMap.get(v.vote_pubkey) === tierFilter)
    }
    return list
  }, [data, tierFilter])

  const sortedValidators = useMemo(() => {
    const list = [...filtered]
    list.sort((a, b) => {
      const av = getSortValue(a, sortKey)
      const bv = getSortValue(b, sortKey)
      if (typeof av === 'string' && typeof bv === 'string') {
        return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      }
      if (typeof av === 'boolean' && typeof bv === 'boolean') {
        return sortDir === 'asc' ? Number(av) - Number(bv) : Number(bv) - Number(av)
      }
      return sortDir === 'asc' ? Number(av) - Number(bv) : Number(bv) - Number(av)
    })
    return list
  }, [filtered, sortKey, sortDir])

  const toggleSort = useCallback((key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }, [sortKey])

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load validator data</div>
          <div className="text-sm text-muted-foreground">{(error as Error)?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  if (!data) return null

  const SortIcon = ({ col }: { col: SortKey }) => {
    if (sortKey !== col) return null
    return sortDir === 'asc'
      ? <ChevronUp className="inline h-3.5 w-3.5 ml-0.5" />
      : <ChevronDown className="inline h-3.5 w-3.5 ml-0.5" />
  }

  return (
    <div className="flex-1 overflow-y-auto p-4 space-y-4">
      {/* Filter row */}
      <div className="flex items-center gap-3 flex-wrap">
        <select value={metro} onChange={e => setMetro(e.target.value)}
          className="rounded-md border border-border bg-card px-3 py-1.5 text-sm">
          <option value="">All metros</option>
          {data.metro_breakdown.map(m => (
            <option key={m.metro_code} value={m.metro_code}>{m.metro_code}</option>
          ))}
        </select>

        <select value={tierFilter} onChange={e => setTierFilter(e.target.value)}
          className="rounded-md border border-border bg-card px-3 py-1.5 text-sm">
          <option value="">All tiers</option>
          <option value="super">Super (top 33%)</option>
          <option value="high">High (33–66%)</option>
          <option value="mid">Mid (bottom 34%)</option>
        </select>

        <div className="flex rounded-md border border-border overflow-hidden">
          {(['all', 'on', 'off'] as const).map(opt => (
            <button key={opt} onClick={() => setDzFilter(opt === 'all' ? undefined : opt)}
              className={`px-3 py-1.5 text-sm ${(opt === 'all' && !dzFilter) || dzFilter === opt ? 'bg-background text-foreground' : 'bg-card text-muted-foreground hover:text-foreground'}`}>
              {opt === 'all' ? 'All' : opt === 'on' ? 'On DZ' : 'Off DZ'}
            </button>
          ))}
        </div>

        <span className="ml-auto text-sm text-muted-foreground">
          Showing {sortedValidators.length} of {data.total_validators.toLocaleString()} measured validators
        </span>
      </div>

      {/* Scatter map */}
      <ValidatorScatterMap data={data} validators={sortedValidators} metroCoords={metroCoords} />

      {/* Validator table */}
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="px-5 py-3 border-b border-border flex items-center justify-between">
          <h3 className="text-sm font-medium">Top Validators</h3>
          <FreshnessIndicator dataUpdatedAt={dataUpdatedAt} />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left text-[11px] text-muted-foreground uppercase tracking-wider">
                <th className="px-4 py-2.5 font-medium">#</th>
                <th className="px-4 py-2.5 font-medium cursor-pointer select-none" onClick={() => toggleSort('name')}>Name<SortIcon col="name" /></th>
                <th className="px-4 py-2.5 font-medium cursor-pointer select-none" onClick={() => toggleSort('metro_code')}>Metro<SortIcon col="metro_code" /></th>
                <th className="px-4 py-2.5 font-medium cursor-pointer select-none" onClick={() => toggleSort('datacenter')}>Datacenter<SortIcon col="datacenter" /></th>
                <th className="px-4 py-2.5 font-medium cursor-pointer select-none text-right" onClick={() => toggleSort('stake_sol')}>Stake<SortIcon col="stake_sol" /></th>
                <th className="px-4 py-2.5 font-medium cursor-pointer select-none text-right" onClick={() => toggleSort('commission')}>Commission<SortIcon col="commission" /></th>
                <th className="px-4 py-2.5 font-medium cursor-pointer select-none" onClick={() => toggleSort('is_dz')}>DZ<SortIcon col="is_dz" /></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {sortedValidators.map((v, i) => (
                <tr key={v.vote_pubkey} className="hover:bg-muted/50">
                  <td className="px-4 py-2.5 tabular-nums text-muted-foreground">{i + 1}</td>
                  <td className="px-4 py-2.5 font-medium truncate max-w-[200px]">
                    {v.name || `${v.vote_pubkey.slice(0, 4)}...${v.vote_pubkey.slice(-4)}`}
                  </td>
                  <td className="px-4 py-2.5">{v.metro_code}</td>
                  <td className="px-4 py-2.5 truncate max-w-[160px]">{v.datacenter}</td>
                  <td className="px-4 py-2.5 text-right tabular-nums">{formatSol(v.stake_sol)} <span className="text-muted-foreground">({formatPct(v.stake_pct)})</span></td>
                  <td className="px-4 py-2.5 text-right tabular-nums">{v.commission}%</td>
                  <td className="px-4 py-2.5">
                    {v.is_dz
                      ? <span className="text-[11px] font-medium px-2 py-0.5 rounded-full bg-green-500/10 text-green-600 dark:text-green-400">Yes</span>
                      : <span className="text-[11px] text-muted-foreground">No</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {/* Ghost rows with CTA overlay */}
        <div className="relative">
          <div className="blur-[4px] opacity-50 pointer-events-none select-none overflow-hidden">
            <table className="w-full text-sm">
              <tbody className="divide-y divide-border">
                {GHOST_ROWS.map((row, i) => (
                  <tr key={`ghost-${i}`}>
                    <td className="px-4 py-2.5 tabular-nums text-muted-foreground">{sortedValidators.length + i + 1}</td>
                    <td className="px-4 py-2.5 font-medium">{row.name}</td>
                    <td className="px-4 py-2.5">{row.metro}</td>
                    <td className="px-4 py-2.5">{row.dc}</td>
                    <td className="px-4 py-2.5 text-right tabular-nums">{formatSol(row.stake)} <span className="text-muted-foreground">({formatPct(row.pct)})</span></td>
                    <td className="px-4 py-2.5 text-right tabular-nums">{row.comm}%</td>
                    <td className="px-4 py-2.5">
                      {row.dz
                        ? <span className="text-[11px] font-medium px-2 py-0.5 rounded-full bg-green-500/10 text-green-600 dark:text-green-400">Yes</span>
                        : <span className="text-[11px] text-muted-foreground">No</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="bg-card/95 backdrop-blur-sm border border-border rounded-lg px-6 py-4 text-center shadow-lg">
              <div className="text-sm font-medium mb-1">Want full validator geolocation data?</div>
              <div className="text-[12px] text-muted-foreground mb-3">Get access to complete validator positioning and analytics.</div>
              <a href="https://doublezero.xyz/geolocation-interest" target="_blank" rel="noopener noreferrer"
                className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 transition-colors">
                Request access <ExternalLink className="h-3.5 w-3.5" />
              </a>
            </div>
          </div>
        </div>
      </div>

      {/* Stake tier breakdown */}
      <StakeTierBreakdown data={data} metroCoords={metroCoords} donutMode={donutMode} setDonutMode={setDonutMode} />

      {/* CTA banner */}
      <div className="rounded-lg border border-blue-500/20 bg-blue-500/5 px-5 py-4">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div>
            <div className="text-sm font-medium">Interested in geolocation for DoubleZero?</div>
            <div className="text-[12px] text-muted-foreground mt-1">Help improve network decentralization by participating in the geolocation program.</div>
          </div>
          <a href="https://doublezero.xyz/geolocation-interest" target="_blank" rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 transition-colors flex-shrink-0">
            Learn more <ExternalLink className="h-3.5 w-3.5" />
          </a>
        </div>
      </div>
    </div>
  )
}
