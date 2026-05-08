import { useMemo, useState, useCallback } from 'react'
import MapGL, { Source, Layer } from 'react-map-gl/maplibre'
import type { MapLayerMouseEvent } from 'react-map-gl/maplibre'
import type { StyleSpecification } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useQuery } from '@tanstack/react-query'
import { useTheme } from '@/hooks/use-theme'
import { Loader2, AlertCircle, AlertTriangle, ArrowRight, ExternalLink } from 'lucide-react'
import { ResponsiveContainer, BarChart, Bar, Cell, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts'
import { fetchGeoConcentration, fetchMetros, type GeoConcentrationResponse } from '@/lib/api'

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

const WARN_TOP_TWO_METROS_PCT = 33
const WARN_COUNTRY_PCT = 8
const WARN_ASN_PCT = 10
const WARN_MAX_ASN_PCT = 20

function formatPct(v: number): string {
  return v < 0.1 ? '<0.1%' : `${v.toFixed(1)}%`
}

function formatSol(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
  if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`
  return v.toFixed(0)
}

function StatCard({ label, value, warning }: { label: string; value: React.ReactNode; warning?: string }) {
  return (
    <div className="rounded-lg border border-border bg-card px-5 py-5 min-w-0">
      <div className="text-[10px] font-medium text-muted-foreground/50 uppercase tracking-widest mb-3">{label}</div>
      <div className="text-xl sm:text-2xl font-semibold tabular-nums tracking-tight">{value}</div>
      {warning && (
        <div className="flex items-center gap-1.5 mt-2 text-[11px] text-amber-600 dark:text-amber-400">
          <AlertTriangle className="h-3 w-3 flex-shrink-0" />
          <span>{warning}</span>
        </div>
      )}
    </div>
  )
}

function AnchorPointMap({ data, metroCoords }: {
  data: GeoConcentrationResponse
  metroCoords: Map<string, { lat: number; lng: number; name: string }>
}) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const mapStyle = useMemo(() => createMapStyle(isDark), [isDark])
  const [hoverInfo, setHoverInfo] = useState<{
    x: number; y: number; code: string; name: string
    stakeSol: number; stakePct: number; validators: number
  } | null>(null)

  const pointsGeoJSON = useMemo(() => ({
    type: 'FeatureCollection' as const,
    features: data.metros
      .map((m) => {
        const coords = metroCoords.get(m.metro_code)
        if (!coords) return null
        return {
          type: 'Feature' as const,
          properties: {
            code: m.metro_code, name: coords.name, stake_sol: m.stake_sol,
            stake_pct: m.stake_pct, validator_count: m.validators,
            radius: Math.max(4, Math.sqrt(m.stake_pct) * 6),
          },
          geometry: { type: 'Point' as const, coordinates: [coords.lng, coords.lat] },
        }
      })
      .filter((f): f is NonNullable<typeof f> => f !== null),
  }), [data.metros, metroCoords])

  const onHover = useCallback((event: MapLayerMouseEvent) => {
    const feature = event.features?.[0]
    if (feature) {
      setHoverInfo({
        x: event.point.x, y: event.point.y,
        code: String(feature.properties?.code ?? ''),
        name: String(feature.properties?.name ?? ''),
        stakeSol: Number(feature.properties?.stake_sol ?? 0),
        stakePct: Number(feature.properties?.stake_pct ?? 0),
        validators: Number(feature.properties?.validator_count ?? 0),
      })
    } else {
      setHoverInfo(null)
    }
  }, [])

  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-5 py-3 border-b border-border">
        <h3 className="text-sm font-medium">Anchor Point Distribution</h3>
      </div>
      <div className="relative h-[400px]">
        <MapGL
          initialViewState={{ longitude: 0, latitude: 20, zoom: 1.5 }}
          style={{ width: '100%', height: '100%' }}
          mapStyle={mapStyle}
          interactiveLayerIds={['metro-circles']}
          onMouseMove={onHover}
          onMouseLeave={() => setHoverInfo(null)}
        >
          <Source id="metro-points" type="geojson" data={pointsGeoJSON}>
            <Layer id="metro-circles" type="circle" paint={{
              'circle-radius': ['get', 'radius'], 'circle-color': '#3b82f6',
              'circle-opacity': 0.7, 'circle-stroke-width': 1.5,
              'circle-stroke-color': isDark ? '#1e3a5f' : '#93c5fd',
            }} />
            <Layer id="metro-labels" type="symbol"
              layout={{ 'text-field': ['get', 'code'], 'text-size': 10, 'text-offset': [0, 1.8], 'text-anchor': 'top' }}
              paint={{
                'text-color': isDark ? '#e2e8f0' : '#1e293b',
                'text-halo-color': isDark ? '#0f172a' : '#ffffff', 'text-halo-width': 1,
              }} />
          </Source>
        </MapGL>
        {pointsGeoJSON.features.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <div className="bg-card/90 backdrop-blur-sm border border-border rounded-lg px-6 py-4 text-center">
              <div className="text-sm text-muted-foreground">No metro coordinate data available</div>
            </div>
          </div>
        )}
        {hoverInfo && (
          <div className="absolute z-10 pointer-events-none bg-popover border border-border rounded-lg px-3 py-2 shadow-lg text-sm"
            style={{ left: hoverInfo.x + 12, top: hoverInfo.y - 12 }}>
            <div className="font-medium">{hoverInfo.code}</div>
            <div className="text-muted-foreground">{hoverInfo.name}</div>
            <div className="text-muted-foreground">Stake: {formatSol(hoverInfo.stakeSol)} SOL ({formatPct(hoverInfo.stakePct)})</div>
            <div className="text-muted-foreground">Validators: {hoverInfo.validators}</div>
          </div>
        )}
      </div>
    </div>
  )
}

function CountryBarChart({ data }: { data: GeoConcentrationResponse }) {
  const sorted = useMemo(
    () => [...data.countries].sort((a, b) => b.stake_pct - a.stake_pct)
      .map((c) => ({ ...c, fill: c.stake_pct > WARN_COUNTRY_PCT ? '#f59e0b' : '#3b82f6' })),
    [data.countries],
  )

  if (sorted.length === 0) {
    return <div className="rounded-lg border border-border bg-card px-5 py-8 text-center text-sm text-muted-foreground">No country data available</div>
  }

  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden flex flex-col max-h-[500px]">
      <div className="px-5 py-3 border-b border-border flex items-center justify-between flex-shrink-0">
        <h3 className="text-sm font-medium">Stake by Country</h3>
        <span className="text-[11px] text-muted-foreground">Warning threshold: {WARN_COUNTRY_PCT}%</span>
      </div>
      <div className="px-5 py-4 overflow-y-auto">
        <ResponsiveContainer width="100%" height={sorted.length * 28 + 20}>
          <BarChart data={sorted} layout="vertical" margin={{ top: 0, right: 40, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
            <XAxis type="number" tickLine={false} axisLine={false} tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${v}%`} />
            <YAxis dataKey="country_name" type="category" tickLine={false} axisLine={false} tick={{ fontSize: 11 }} width={50} />
            <Tooltip cursor={{ fill: 'var(--muted)', opacity: 0.4 }} formatter={(value) => [`${Number(value).toFixed(1)}%`, 'Stake']} />
            <Bar dataKey="stake_pct" radius={[0, 3, 3, 0]}>
              {sorted.map((entry, i) => <Cell key={i} fill={entry.fill} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function AsnList({ data }: { data: GeoConcentrationResponse }) {
  const sorted = useMemo(() => [...data.asns].sort((a, b) => b.stake_pct - a.stake_pct), [data.asns])

  if (sorted.length === 0) {
    return <div className="rounded-lg border border-border bg-card px-5 py-8 text-center text-sm text-muted-foreground">No ASN data available</div>
  }

  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden flex flex-col max-h-[500px]">
      <div className="px-5 py-3 border-b border-border flex-shrink-0">
        <h3 className="text-sm font-medium">ASN Concentration</h3>
      </div>
      <div className="divide-y divide-border overflow-y-auto">
        {sorted.map((asn) => {
          const concentrated = asn.stake_pct > WARN_ASN_PCT
          return (
            <div key={asn.asn} className="px-5 py-3 flex items-center justify-between gap-4">
              <div className="min-w-0">
                <div className="text-sm font-medium truncate">{asn.asn_org}</div>
                <div className="text-[11px] text-muted-foreground">AS{asn.asn} · {asn.validators} validators · {formatSol(asn.stake_sol)} SOL</div>
              </div>
              <div className="flex items-center gap-3 flex-shrink-0">
                <span className="text-sm font-medium tabular-nums">{formatPct(asn.stake_pct)}</span>
                <span className={`text-[10px] font-medium px-2 py-0.5 rounded-full ${concentrated ? 'bg-red-500/10 text-red-600 dark:text-red-400' : 'bg-green-500/10 text-green-600 dark:text-green-400'}`}>
                  {concentrated ? 'concentrated' : 'normal'}
                </span>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

const HOW_IT_WORKS_STEPS = [
  { label: 'Geoprobes', desc: 'Distributed measurement nodes' },
  { label: 'Latency', desc: 'Round-trip time measurements' },
  { label: 'Metro Assignment', desc: 'Map validators to anchor points' },
  { label: 'Concentration', desc: 'Analyze geographic distribution' },
]

function HowItWorks() {
  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-5 py-3 border-b border-border">
        <h3 className="text-sm font-medium">How It Works</h3>
      </div>
      <div className="px-5 py-4 flex items-center justify-between gap-2 overflow-x-auto">
        {HOW_IT_WORKS_STEPS.map((step, i) => (
          <div key={i} className="flex items-center gap-2 min-w-0">
            <div className="flex-shrink-0 text-center">
              <div className="w-8 h-8 rounded-full bg-blue-500/10 text-blue-600 dark:text-blue-400 flex items-center justify-center text-sm font-medium">{i + 1}</div>
              <div className="text-[11px] font-medium mt-1.5">{step.label}</div>
              <div className="text-[10px] text-muted-foreground">{step.desc}</div>
            </div>
            {i < HOW_IT_WORKS_STEPS.length - 1 && <ArrowRight className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />}
          </div>
        ))}
      </div>
    </div>
  )
}

export function DzdpConcentrationView() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['geo-concentration'],
    queryFn: fetchGeoConcentration,
    refetchInterval: 60_000,
  })

  const { data: metrosData } = useQuery({
    queryKey: ['metros-for-concentration'],
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
          <div className="text-lg font-medium mb-2">Unable to load concentration data</div>
          <div className="text-sm text-muted-foreground">{(error as Error)?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  if (!data) return null

  return (
    <div className="flex-1 overflow-y-auto p-4 space-y-4">
      <div className="grid grid-cols-1 xs:grid-cols-2 xl:grid-cols-4 gap-3">
        <StatCard label="Validators Measured" value={data.hero_stats.validators_measured.toLocaleString()} />
        <StatCard label="Top 2 Metros Stake" value={formatPct(data.hero_stats.stake_top_two_metros_pct)}
          warning={data.hero_stats.stake_top_two_metros_pct > WARN_TOP_TWO_METROS_PCT ? `Exceeds ${WARN_TOP_TWO_METROS_PCT}% threshold` : undefined} />
        <StatCard label="Anchor Points" value={data.hero_stats.anchor_points} />
        <StatCard label="Max ASN Stake" value={formatPct(data.hero_stats.stake_max_asn_pct)}
          warning={data.hero_stats.stake_max_asn_pct > WARN_MAX_ASN_PCT ? `Exceeds ${WARN_MAX_ASN_PCT}% threshold` : undefined} />
      </div>

      <AnchorPointMap data={data} metroCoords={metroCoords} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <CountryBarChart data={data} />
        <AsnList data={data} />
      </div>

      <HowItWorks />

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
