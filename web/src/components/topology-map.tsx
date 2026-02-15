import { useMemo, useEffect, useRef, useState, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import MapGL, { Source, Layer, Marker } from 'react-map-gl/maplibre'
import type { MapRef, MapLayerMouseEvent, LngLatBoundsLike } from 'react-map-gl/maplibre'
import type { StyleSpecification } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useQuery } from '@tanstack/react-query'
import { useTheme } from '@/hooks/use-theme'
import type { TopologyMetro, TopologyDevice, TopologyLink, TopologyValidator, MultiPathResponse, SimulateLinkRemovalResponse, SimulateLinkAdditionResponse, WhatIfRemovalResponse, MetroDevicePathsResponse, MulticastGroupDetail, MulticastTreeResponse } from '@/lib/api'
import { fetchISISPaths, fetchISISTopology, fetchCriticalLinks, fetchSimulateLinkRemoval, fetchSimulateLinkAddition, fetchWhatIfRemoval, fetchLinkHealth, fetchTopologyCompare, fetchMetroDevicePaths, fetchMulticastGroup, fetchMulticastTreePaths } from '@/lib/api'
import { useTopology, TopologyControlBar, TopologyPanel, DeviceDetails, LinkDetails, MetroDetails, ValidatorDetails, EntityLink as TopologyEntityLink, PathModePanel, MetroPathModePanel, CriticalityPanel, WhatIfRemovalPanel, WhatIfAdditionPanel, ImpactPanel, ComparePanel, StakeOverlayPanel, LinkHealthOverlayPanel, TrafficFlowOverlayPanel, MetroClusteringOverlayPanel, ContributorsOverlayPanel, ValidatorsOverlayPanel, DeviceTypeOverlayPanel, LinkTypeOverlayPanel, MulticastTreesOverlayPanel, LINK_TYPE_COLORS, MULTICAST_PUBLISHER_COLORS, type DeviceOption, type MetroOption } from '@/components/topology'

// Path colors for multi-path visualization
const PATH_COLORS = [
  '#22c55e',  // green - primary/shortest
  '#3b82f6',  // blue - alternate 1
  '#a855f7',  // purple - alternate 2
  '#f97316',  // orange - alternate 3
  '#06b6d4',  // cyan - alternate 4
]

// Metro colors for metro clustering visualization (10 distinct colors)
const METRO_COLORS = [
  '#3b82f6',  // blue
  '#a78bfa',  // purple
  '#f472b6',  // pink
  '#f97316',  // orange
  '#22c55e',  // green
  '#22d3ee',  // cyan
  '#818cf8',  // indigo
  '#facc15',  // yellow
  '#2dd4bf',  // teal
  '#f472b6',  // rose
]

// Contributor colors for contributor overlay visualization (12 distinct colors)
const CONTRIBUTOR_COLORS = [
  '#8b5cf6',  // violet
  '#ec4899',  // pink
  '#06b6d4',  // cyan
  '#84cc16',  // lime
  '#f59e0b',  // amber
  '#6366f1',  // indigo
  '#14b8a6',  // teal
  '#f97316',  // orange
  '#a855f7',  // purple
  '#10b981',  // emerald
  '#ef4444',  // red
  '#0ea5e9',  // sky
]

// Device type colors (hybrid, transit, edge)
// Avoid green/red (status colors) and blue/purple (link colors)
const DEVICE_TYPE_COLORS: Record<string, string> = {
  hybrid: '#eab308',    // yellow
  transit: '#f97316',   // orange
  edge: '#22d3ee',      // cyan
  default: '#9ca3af',   // gray
}

// Interpolate a position along a polyline path at parameter t (0..1)
function interpolateAlongPath(coords: [number, number][], t: number): [number, number] {
  const n = coords.length
  if (n === 0) return [0, 0]
  if (n === 1) return coords[0]
  const idx = t * (n - 1)
  const i = Math.min(Math.floor(idx), n - 2)
  const frac = idx - i
  return [
    coords[i][0] + (coords[i + 1][0] - coords[i][0]) * frac,
    coords[i][1] + (coords[i + 1][1] - coords[i][1]) * frac,
  ]
}

interface TopologyMapProps {
  metros: TopologyMetro[]
  devices: TopologyDevice[]
  links: TopologyLink[]
  validators: TopologyValidator[]
}

// Format bandwidth for display
function formatBandwidth(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

// Format traffic rate for display
function formatTrafficRate(bps: number | undefined | null): string {
  if (bps == null || bps <= 0) return 'N/A'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(2)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(2)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(2)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(2)} Kbps`
  return `${bps.toFixed(0)} bps`
}

// Get link weight based on bandwidth capacity (most links are 10-100 Gbps)
function getBandwidthWeight(bps: number): number {
  const gbps = bps / 1e9
  if (gbps >= 100) return 6
  if (gbps >= 40) return 4
  if (gbps >= 10) return 2
  if (gbps >= 1) return 1.5
  return 1
}

// Calculate validator marker radius based on stake (logarithmic scale)
// Range: 3px (small) to 10px (large)
function calculateValidatorRadius(stakeSol: number): number {
  if (stakeSol <= 0) return 3
  // Log scale: 1k SOL = 3, 10k = 4.5, 100k = 6, 1M = 7.5, 10M = 9
  const minRadius = 3
  const maxRadius = 10
  const minStake = 1000 // 1k SOL
  const radius = minRadius + Math.log10(Math.max(minStake, stakeSol) / minStake) * 1.5
  return Math.min(maxRadius, radius)
}

// Calculate device marker size based on stake (for stake overlay mode)
// Range: 8px (no stake) to 28px (high stake)
function calculateDeviceStakeSize(stakeSol: number): number {
  if (stakeSol <= 0) return 8
  // Log scale: 10k = 10, 100k = 14, 1M = 18, 10M = 22, 100M = 26
  const minSize = 8
  const maxSize = 28
  const minStake = 10000 // 10k SOL
  const size = minSize + Math.log10(Math.max(minStake, stakeSol) / minStake) * 4.5
  return Math.min(maxSize, size)
}

// Get stake-based color intensity (yellow to orange gradient based on stake share)
function getStakeColor(stakeShare: number): string {
  if (stakeShare <= 0) return '#6b7280' // gray for no stake
  // Scale: 0% = yellow, 1%+ = deep orange
  const t = Math.min(stakeShare / 1.0, 1) // cap at 1%
  const r = Math.round(234 + t * (234 - 234))
  const g = Math.round(179 - t * (179 - 88))
  const b = Math.round(8 + t * (8 - 8))
  return `rgb(${r}, ${g}, ${b})`
}

// Hovered link info type
interface HoveredLinkInfo {
  pk: string
  code: string
  linkType: string
  bandwidthBps: number
  latencyUs: number
  jitterUs: number
  latencyAtoZUs: number
  jitterAtoZUs: number
  latencyZtoAUs: number
  jitterZtoAUs: number
  lossPercent: number
  inBps: number
  outBps: number
  deviceAPk: string
  deviceACode: string
  interfaceAName: string
  interfaceAIP: string
  deviceZPk: string
  deviceZCode: string
  interfaceZName: string
  interfaceZIP: string
  contributorPk: string
  contributorCode: string
  sampleCount: number
  committedRttNs: number
  isisDelayOverrideNs: number
  health?: {
    status: string
    committedRttNs: number
    slaRatio: number
    lossPct: number
  }
  // Inter-metro link properties
  isInterMetro?: boolean
  linkCount?: number
  avgLatencyUs?: number
}

// Hovered device info type
interface HoveredDeviceInfo {
  pk: string
  code: string
  deviceType: string
  status: string
  metroPk: string
  metroName: string
  contributorPk: string
  contributorCode: string
  userCount: number
  validatorCount: number
  stakeSol: number
  stakeShare: number
  interfaces: { name: string; ip: string; status: string }[]
}

// Hovered metro info type
interface HoveredMetroInfo {
  pk: string
  code: string
  name: string
  deviceCount: number
}

// Hovered validator info type
interface HoveredValidatorInfo {
  votePubkey: string
  nodePubkey: string
  tunnelId: number
  city: string
  country: string
  stakeSol: string
  stakeShare: string
  commission: number
  version: string
  gossipIp: string
  gossipPort: number
  tpuQuicIp: string
  tpuQuicPort: number
  deviceCode: string
  devicePk: string
  metroPk: string
  metroName: string
  inRate: string
  outRate: string
}

// Selected item type for drawer
type SelectedItem =
  | { type: 'link'; data: HoveredLinkInfo }
  | { type: 'device'; data: HoveredDeviceInfo }
  | { type: 'metro'; data: HoveredMetroInfo }
  | { type: 'validator'; data: HoveredValidatorInfo }

// Calculate device position with radial offset for multiple devices at same metro
function calculateDevicePosition(
  metroLat: number,
  metroLng: number,
  deviceIndex: number,
  totalDevices: number
): [number, number] {
  if (totalDevices === 1) {
    return [metroLng, metroLat]
  }

  // Distribute devices in a circle around metro center
  const radius = 0.3 // degrees offset
  const angle = (2 * Math.PI * deviceIndex) / totalDevices
  const latOffset = radius * Math.cos(angle)
  // Adjust for latitude distortion
  const lngOffset = radius * Math.sin(angle) / Math.cos(metroLat * Math.PI / 180)

  return [metroLng + lngOffset, metroLat + latOffset]
}

// Get per-publisher curve offset for parallel multicast tree lines.
// Distributes N publishers symmetrically around the default curve (0.15).
function getPublisherOffset(publisherIndex: number, totalPublishers: number): number {
  if (totalPublishers === 1) return 0.15
  const spread = 0.06
  const center = 0.15
  const start = center - (spread * (totalPublishers - 1)) / 2
  return start + spread * publisherIndex
}

// Calculate curved path between two points (returns GeoJSON coordinates [lng, lat])
function calculateCurvedPath(
  start: [number, number],
  end: [number, number],
  curveOffset: number = 0.15
): [number, number][] {
  const startLng = start[0]
  let endLng = end[0]
  const lngDelta = endLng - startLng
  if (Math.abs(lngDelta) > 180) {
    // Cross the antimeridian in the shorter direction (fixes eastward long routes).
    endLng = lngDelta > 0 ? endLng - 360 : endLng + 360
  }

  const midLng = (startLng + endLng) / 2
  const midLat = (start[1] + end[1]) / 2

  // Calculate perpendicular offset for curve
  const dx = endLng - startLng
  const dy = end[1] - start[1]
  const length = Math.sqrt(dx * dx + dy * dy)

  if (length === 0) return [start, end]

  const controlLng = midLng - (dy / length) * curveOffset * length
  const controlLat = midLat + (dx / length) * curveOffset * length

  // Generate points along quadratic bezier curve
  const points: [number, number][] = []
  const segments = 20
  for (let i = 0; i <= segments; i++) {
    const t = i / segments
    const lng = (1 - t) * (1 - t) * startLng + 2 * (1 - t) * t * controlLng + t * t * endLng
    const lat = (1 - t) * (1 - t) * start[1] + 2 * (1 - t) * t * controlLat + t * t * end[1]
    points.push([lng, lat])
  }
  return points
}

// Create MapLibre style with CARTO basemap
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
    layers: [
      {
        id: 'carto-tiles',
        type: 'raster',
        source: 'carto',
        minzoom: 0,
        maxzoom: 22,
      },
    ],
  }
}

export function TopologyMap({ metros, devices, links, validators }: TopologyMapProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const [searchParams, setSearchParams] = useSearchParams()
  const [hoveredLink, setHoveredLink] = useState<HoveredLinkInfo | null>(null)
  const [hoveredDevice, setHoveredDevice] = useState<HoveredDeviceInfo | null>(null)
  const [hoveredMetro, setHoveredMetro] = useState<HoveredMetroInfo | null>(null)
  const [hoveredValidator, setHoveredValidator] = useState<HoveredValidatorInfo | null>(null)
  const [mousePos, setMousePos] = useState<{ x: number; y: number }>({ x: 0, y: 0 })
  const [selectedItem, setSelectedItemState] = useState<SelectedItem | null>(null)
  const mapRef = useRef<MapRef>(null)
  const [mapReady, setMapReady] = useState(false)
  const markerClickedRef = useRef(false)

  // Get unified topology context
  const { mode, setMode, pathMode, setPathMode, overlays, toggleOverlay, panel, openPanel, closePanel, selection, impactDevices, toggleImpactDevice, clearImpactDevices } = useTopology()

  // Derive mode states from context
  const pathModeEnabled = mode === 'path'
  const whatifRemovalMode = mode === 'whatif-removal'
  const whatifAdditionMode = mode === 'whatif-addition'
  const impactMode = mode === 'impact'

  // Derive overlay states from context
  const criticalityOverlayEnabled = overlays.criticality
  const showValidators = overlays.validators
  const deviceTypeMode = overlays.deviceType
  const linkTypeMode = overlays.linkType
  const stakeOverlayMode = overlays.stake
  const linkHealthMode = overlays.linkHealth
  const trafficFlowMode = overlays.trafficFlow
  const metroClusteringMode = overlays.metroClustering
  const contributorDevicesMode = overlays.contributorDevices
  const contributorLinksMode = overlays.contributorLinks
  const bandwidthMode = overlays.bandwidth
  const isisHealthMode = overlays.isisHealth
  const multicastTreesMode = overlays.multicastTrees

  // Multicast trees operational state (local)
  const [selectedMulticastGroup, setSelectedMulticastGroup] = useState<string | null>(null)
  const [multicastGroupDetails, setMulticastGroupDetails] = useState<Map<string, MulticastGroupDetail>>(new Map())
  const [multicastTreePaths, setMulticastTreePaths] = useState<Map<string, MulticastTreeResponse>>(new Map())
  const [enabledPublishers, setEnabledPublishers] = useState<Set<string>>(new Set())
  const [enabledSubscribers, setEnabledSubscribers] = useState<Set<string>>(new Set())
  // PKs to skip during auto-enable (restored from URL on initial load)
  const initialDisabledPubsRef = useRef<Set<string> | null>(null)
  const initialDisabledSubsRef = useRef<Set<string> | null>(null)
  const [dimOtherLinks, setDimOtherLinks] = useState(true)
  const [animateFlow, setAnimateFlow] = useState(true)
  const [showTreeValidators, setShowTreeValidators] = useState(true)
  const [linkAnimating, setLinkAnimating] = useState(true)


  // Handler to select multicast group
  const handleSelectMulticastGroup = useCallback((code: string | null) => {
    setSelectedMulticastGroup(code)
  }, [])

  // Handler to toggle individual publisher
  const handleTogglePublisher = useCallback((devicePK: string) => {
    setEnabledPublishers(prev => {
      const next = new Set(prev)
      if (next.has(devicePK)) {
        next.delete(devicePK)
      } else {
        next.add(devicePK)
      }
      return next
    })
  }, [])

  // Handler to toggle individual subscriber
  const handleToggleSubscriber = useCallback((devicePK: string) => {
    setEnabledSubscribers(prev => {
      const next = new Set(prev)
      if (next.has(devicePK)) {
        next.delete(devicePK)
      } else {
        next.add(devicePK)
      }
      return next
    })
  }, [])


  // Fetch multicast tree paths when group is selected
  useEffect(() => {
    if (!multicastTreesMode || !selectedMulticastGroup) return

    // Fetch tree paths for selected group if we don't have it yet
    if (multicastTreePaths.has(selectedMulticastGroup)) return
    const code = selectedMulticastGroup
    fetchMulticastTreePaths(code)
      .then(result => {
        setMulticastTreePaths(prev => new Map(prev).set(code, result))
      })
      .catch(err => console.error(`Failed to fetch multicast tree paths for ${code}:`, err))
  }, [multicastTreesMode, selectedMulticastGroup, multicastTreePaths])

  // Auto-load group details when group is selected
  useEffect(() => {
    if (!multicastTreesMode || !selectedMulticastGroup) return
    if (multicastGroupDetails.has(selectedMulticastGroup)) return
    const code = selectedMulticastGroup
    fetchMulticastGroup(code)
      .then(detail => setMulticastGroupDetails(prev => new Map(prev).set(code, detail)))
      .catch(err => console.error('Failed to fetch multicast group:', err))
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails])

  // Set enabled publishers/subscribers when group details are loaded
  useEffect(() => {
    if (!multicastTreesMode || !selectedMulticastGroup) return
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return

    // On first load, skip PKs that were disabled in the URL
    const skipPubs = initialDisabledPubsRef.current
    const skipSubs = initialDisabledSubsRef.current
    initialDisabledPubsRef.current = null
    initialDisabledSubsRef.current = null

    const pubs = new Set<string>()
    const subs = new Set<string>()
    detail.members.forEach(m => {
      if ((m.mode === 'P' || m.mode === 'P+S') && !skipPubs?.has(m.device_pk)) {
        pubs.add(m.device_pk)
      }
      if ((m.mode === 'S' || m.mode === 'P+S') && !skipSubs?.has(m.device_pk)) {
        subs.add(m.device_pk)
      }
    })
    setEnabledPublishers(pubs)
    setEnabledSubscribers(subs)
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails])

  // Path finding operational state (local)
  const [pathSource, setPathSource] = useState<string | null>(null)
  const [pathTarget, setPathTarget] = useState<string | null>(null)
  const [pathsResult, setPathsResult] = useState<MultiPathResponse | null>(null)
  const [pathLoading, setPathLoading] = useState(false)
  const [selectedPathIndex, setSelectedPathIndex] = useState(0)

  // Reverse path state
  const [showReverse, setShowReverse] = useState(true)
  const [reversePathsResult, setReversePathsResult] = useState<MultiPathResponse | null>(null)
  const [selectedReversePathIndex, setSelectedReversePathIndex] = useState<number>(0)
  const [reversePathLoading, setReversePathLoading] = useState(false)

  // Metro path finding state
  const [metroPathSource, setMetroPathSource] = useState<string | null>(null)
  const [metroPathTarget, setMetroPathTarget] = useState<string | null>(null)
  const [metroPathsResult, setMetroPathsResult] = useState<MetroDevicePathsResponse | null>(null)
  const [metroPathLoading, setMetroPathLoading] = useState(false)
  const [metroPathViewMode, setMetroPathViewMode] = useState<'aggregate' | 'drilldown'>('aggregate')
  const [metroPathSelectedPairs, setMetroPathSelectedPairs] = useState<number[]>([])

  // Handler to toggle pair selection (up to 5 pairs)
  const handleToggleMetroPathPair = useCallback((index: number) => {
    setMetroPathSelectedPairs(prev => {
      if (prev.includes(index)) {
        return prev.filter(i => i !== index)
      }
      if (prev.length >= 5) return prev // Max 5 selections
      return [...prev, index]
    })
  }, [])

  // What-If Link Removal operational state (local)
  const [removalLink, setRemovalLink] = useState<{ sourcePK: string; targetPK: string; linkPK: string } | null>(null)
  const [removalResult, setRemovalResult] = useState<SimulateLinkRemovalResponse | null>(null)
  const [removalLoading, setRemovalLoading] = useState(false)

  // What-If Link Addition operational state (local)
  const [additionSource, setAdditionSource] = useState<string | null>(null)
  const [additionTarget, setAdditionTarget] = useState<string | null>(null)
  const [additionMetric, setAdditionMetric] = useState<number>(1000)
  const [additionResult, setAdditionResult] = useState<SimulateLinkAdditionResponse | null>(null)
  const [additionLoading, setAdditionLoading] = useState(false)

  // Failure Impact operational state (local) - impactDevices comes from context
  const [impactResult, setImpactResult] = useState<WhatIfRemovalResponse | null>(null)
  const [impactLoading, setImpactLoading] = useState(false)

  // Metro Clustering operational state (local)
  const [collapsedMetros, setCollapsedMetros] = useState<Set<string>>(new Set())

  // Fetch ISIS topology to determine which devices have ISIS data
  const { data: isisTopology } = useQuery({
    queryKey: ['isis-topology'],
    queryFn: fetchISISTopology,
    enabled: pathModeEnabled,
  })

  // Build set of ISIS-enabled device PKs
  const isisDevicePKs = useMemo(() => {
    if (!isisTopology?.nodes) return new Set<string>()
    return new Set(isisTopology.nodes.map(node => node.data.id))
  }, [isisTopology])

  // Fetch critical links when in criticality mode
  const { data: criticalLinksData } = useQuery({
    queryKey: ['critical-links'],
    queryFn: fetchCriticalLinks,
    enabled: criticalityOverlayEnabled,
  })

  // Fetch link health data when link health mode is enabled
  const { data: linkHealthData } = useQuery({
    queryKey: ['link-health'],
    queryFn: fetchLinkHealth,
    enabled: linkHealthMode,
    staleTime: 30000,
  })

  // Fetch topology comparison when ISIS health overlay is enabled
  const { data: compareData, isLoading: compareLoading } = useQuery({
    queryKey: ['topology-compare'],
    queryFn: fetchTopologyCompare,
    enabled: isisHealthMode,
    refetchInterval: 60000,
  })

  // Build link SLA status map (keyed by link PK)
  const linkSlaStatus = useMemo(() => {
    if (!linkHealthData?.links) return new Map<string, { status: string; avgRttUs: number; committedRttNs: number; lossPct: number; slaRatio: number }>()
    const slaMap = new Map<string, { status: string; avgRttUs: number; committedRttNs: number; lossPct: number; slaRatio: number }>()

    for (const link of linkHealthData.links) {
      slaMap.set(link.link_pk, {
        status: link.sla_status,
        avgRttUs: link.avg_rtt_us,
        committedRttNs: link.committed_rtt_ns,
        lossPct: link.loss_pct,
        slaRatio: link.sla_ratio,
      })
    }
    return slaMap
  }, [linkHealthData])

  // Build edge health status from compare data (keyed by device pair)
  // Returns: 'matched' | 'missing' | 'extra' | 'mismatch' | undefined
  const edgeHealthStatus = useMemo(() => {
    if (!compareData?.discrepancies) return new Map<string, string>()
    const status = new Map<string, string>()

    for (const d of compareData.discrepancies) {
      // Create keys for both directions (using | separator to match link lookup)
      const key1 = `${d.deviceAPK}|${d.deviceBPK}`
      const key2 = `${d.deviceBPK}|${d.deviceAPK}`

      if (d.type === 'missing_isis') {
        status.set(key1, 'missing')
        status.set(key2, 'missing')
      } else if (d.type === 'extra_isis') {
        status.set(key1, 'extra')
        status.set(key2, 'extra')
      } else if (d.type === 'metric_mismatch') {
        status.set(key1, 'mismatch')
        status.set(key2, 'mismatch')
      }
    }
    return status
  }, [compareData])

  // Build link criticality map (keyed by link PK)
  const linkCriticalityMap = useMemo(() => {
    const map = new Map<string, 'critical' | 'important' | 'redundant'>()
    if (!criticalLinksData?.links) return map

    // Build a map from device pair to link PK
    const devicePairToLinkPK = new Map<string, string>()
    for (const link of links) {
      const key1 = `${link.side_a_pk}|${link.side_z_pk}`
      const key2 = `${link.side_z_pk}|${link.side_a_pk}`
      devicePairToLinkPK.set(key1, link.pk)
      devicePairToLinkPK.set(key2, link.pk)
    }

    // Map critical links to link PKs
    for (const critLink of criticalLinksData.links) {
      const key = `${critLink.sourcePK}|${critLink.targetPK}`
      const linkPK = devicePairToLinkPK.get(key)
      if (linkPK) {
        map.set(linkPK, critLink.criticality)
      }
    }
    return map
  }, [criticalLinksData, links])

  // Update URL and panel when selected item changes
  const setSelectedItem = useCallback((item: SelectedItem | null) => {
    settingSelectionLocallyRef.current = true
    setSelectedItemState(item)
    if (item === null) {
      setSearchParams({})
      // Close panel when deselecting (only if showing details, not mode)
      if (panel.content === 'details') {
        closePanel()
      }
    } else {
      const params: Record<string, string> = { type: item.type }
      if (item.type === 'validator') {
        params.id = item.data.votePubkey
      } else if (item.type === 'device') {
        params.id = item.data.pk
      } else if (item.type === 'link') {
        params.id = item.data.pk
      } else if (item.type === 'metro') {
        params.id = item.data.pk
      }
      setSearchParams(params)
      // Open panel to show details (only in explore mode)
      if (mode === 'explore') {
        openPanel('details')
      }
    }
  }, [setSearchParams, panel.content, closePanel, mode, openPanel])

  // Track when we're setting selection locally to avoid sync conflicts
  const settingSelectionLocallyRef = useRef(false)

  // Sync context selection to local state (handles external deselection like closing panel)
  useEffect(() => {
    // Skip if we just set the selection locally (avoid race with URL param update)
    if (settingSelectionLocallyRef.current) {
      settingSelectionLocallyRef.current = false
      return
    }
    if (selection === null && selectedItem !== null) {
      setSelectedItemState(null)
    }
  }, [selection, selectedItem])

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Don't trigger shortcuts when typing in input fields
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return

      const isInMode = mode !== 'explore'

      // Ignore keyboard shortcuts when modifier keys are pressed (e.g., Cmd+R to refresh)
      if (e.metaKey || e.ctrlKey || e.altKey) return

      if (e.key === 'Escape') {
        if (mode !== 'explore') {
          setMode('explore')
        } else if (selectedItem) {
          setSelectedItem(null)
        }
      } else if (e.key === 'p' && !isInMode) {
        setMode('path')
      } else if (e.key === 'c' && !isInMode) {
        toggleOverlay('criticality')
      } else if (e.key === 'r' && !isInMode) {
        setMode('whatif-removal')
      } else if (e.key === 'a' && !isInMode) {
        setMode('whatif-addition')
      } else if (e.key === 's' && !isInMode) {
        toggleOverlay('stake')
      } else if (e.key === 'h' && !isInMode) {
        toggleOverlay('linkHealth')
      } else if (e.key === 't' && !isInMode) {
        toggleOverlay('trafficFlow')
      } else if (e.key === 'm' && !isInMode) {
        toggleOverlay('metroClustering')
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [mode, setMode, toggleOverlay, selectedItem, setSelectedItem])

  // Helper to handle marker clicks - sets flag to prevent map click from clearing selection
  const handleMarkerClick = useCallback((item: SelectedItem) => {
    markerClickedRef.current = true
    setSelectedItem(item)
    setTimeout(() => { markerClickedRef.current = false }, 0)
  }, [setSelectedItem])

  // Hover highlight color: light in dark mode, dark in light mode
  const hoverHighlight = isDark ? '#fff' : '#000'

  // Get metro color based on index
  const getMetroColor = useCallback((_metroPK: string, metroIndex: number) => {
    return METRO_COLORS[metroIndex % METRO_COLORS.length]
  }, [])

  // Toggle metro collapse state
  const toggleMetroCollapse = useCallback((metroPK: string) => {
    setCollapsedMetros(prev => {
      const newSet = new Set(prev)
      if (newSet.has(metroPK)) {
        newSet.delete(metroPK)
      } else {
        newSet.add(metroPK)
      }
      return newSet
    })
  }, [])

  // Get traffic color based on utilization
  const getTrafficColor = useCallback((link: TopologyLink): { color: string; weight: number; opacity: number } => {
    const totalBps = (link.in_bps ?? 0) + (link.out_bps ?? 0)
    const utilization = link.bandwidth_bps > 0 ? (totalBps / link.bandwidth_bps) * 100 : 0

    if (utilization >= 80) {
      return { color: '#ef4444', weight: 5, opacity: 1 } // red - critical
    } else if (utilization >= 50) {
      return { color: '#eab308', weight: 4, opacity: 1 } // yellow - high
    } else if (utilization >= 20) {
      return { color: '#84cc16', weight: 3, opacity: 0.9 } // lime - medium
    } else if (totalBps > 0) {
      return { color: '#22c55e', weight: 2, opacity: 0.8 } // green - low
    } else {
      return { color: isDark ? '#6b7280' : '#9ca3af', weight: 1, opacity: 0.4 } // gray - idle
    }
  }, [isDark])

  // Clear collapsed metros when metro clustering is disabled
  useEffect(() => {
    if (!metroClusteringMode) {
      setCollapsedMetros(new Set())
    }
  }, [metroClusteringMode])

  // Build metro lookup map
  const metroMap = useMemo(() => {
    const map = new Map<string, TopologyMetro>()
    for (const metro of metros) {
      map.set(metro.pk, metro)
    }
    return map
  }, [metros])

  // Build metro index map for consistent colors
  const metroIndexMap = useMemo(() => {
    const map = new Map<string, number>()
    const sortedMetros = [...metros].sort((a, b) => a.code.localeCompare(b.code))
    sortedMetros.forEach((metro, index) => {
      map.set(metro.pk, index)
    })
    return map
  }, [metros])

  // Build device lookup map
  const deviceMap = useMemo(() => {
    const map = new Map<string, TopologyDevice>()
    for (const device of devices) {
      map.set(device.pk, device)
    }
    return map
  }, [devices])

  // Build device code lookup map (pk -> code) for impact panel
  const deviceCodeMap = useMemo(() => {
    const map = new Map<string, string>()
    for (const device of devices) {
      map.set(device.pk, device.code)
    }
    return map
  }, [devices])

  // Build device options for path finding selectors
  const deviceOptions: DeviceOption[] = useMemo(() => {
    return devices
      .map(d => {
        const metro = metros.find(m => m.pk === d.metro_pk)
        return {
          pk: d.pk,
          code: d.code,
          deviceType: d.device_type,
          metro: metro?.code,
        }
      })
      .sort((a, b) => a.code.localeCompare(b.code))
  }, [devices, metros])

  // Build metro options for metro path finding selectors
  const metroOptions: MetroOption[] = useMemo(() => {
    return metros
      .map(m => ({
        pk: m.pk,
        code: m.code,
        name: m.name,
      }))
      .sort((a, b) => a.code.localeCompare(b.code))
  }, [metros])

  // Build link lookup map
  const linkMap = useMemo(() => {
    const map = new Map<string, TopologyLink>()
    for (const link of links) {
      map.set(link.pk, link)
    }
    return map
  }, [links])

  // Build validator lookup map (by vote_pubkey)
  const validatorMap = useMemo(() => {
    const map = new Map<string, TopologyValidator>()
    for (const validator of validators) {
      map.set(validator.vote_pubkey, validator)
    }
    return map
  }, [validators])

  // Group devices by metro
  const devicesByMetro = useMemo(() => {
    const map = new Map<string, TopologyDevice[]>()
    for (const device of devices) {
      const list = map.get(device.metro_pk) || []
      list.push(device)
      map.set(device.metro_pk, list)
    }
    return map
  }, [devices])

  // Build contributor index map (for consistent colors)
  const contributorIndexMap = useMemo(() => {
    const map = new Map<string, number>()
    // Get unique contributors from devices, sorted by code
    const contributorSet = new Map<string, string>() // pk -> code
    for (const device of devices) {
      if (device.contributor_pk) {
        contributorSet.set(device.contributor_pk, device.contributor_code || device.contributor_pk)
      }
    }
    const sorted = [...contributorSet.entries()].sort((a, b) => a[1].localeCompare(b[1]))
    sorted.forEach(([pk], index) => {
      map.set(pk, index)
    })
    return map
  }, [devices])

  // Get contributor color based on index
  const getContributorColor = useCallback((_contributorPK: string, contributorIndex: number) => {
    return CONTRIBUTOR_COLORS[contributorIndex % CONTRIBUTOR_COLORS.length]
  }, [])

  // Group devices by contributor
  const devicesByContributor = useMemo(() => {
    const map = new Map<string, TopologyDevice[]>()
    for (const device of devices) {
      if (device.contributor_pk) {
        const list = map.get(device.contributor_pk) || []
        list.push(device)
        map.set(device.contributor_pk, list)
      }
    }
    return map
  }, [devices])

  // Group links by contributor
  const linksByContributor = useMemo(() => {
    const map = new Map<string, TopologyLink[]>()
    for (const link of links) {
      if (link.contributor_pk) {
        const list = map.get(link.contributor_pk) || []
        list.push(link)
        map.set(link.contributor_pk, list)
      }
    }
    return map
  }, [links])

  // Build contributor info map for overlay panel
  const contributorInfoMap = useMemo(() => {
    const map = new Map<string, { code: string; name: string }>()
    for (const device of devices) {
      if (device.contributor_pk && !map.has(device.contributor_pk)) {
        map.set(device.contributor_pk, {
          code: device.contributor_code || device.contributor_pk.substring(0, 8),
          name: device.contributor_code || '', // Use code as name if no name available
        })
      }
    }
    return map
  }, [devices])

  // Build device stake map for overlay panel (maps device PK to stake info)
  const deviceStakeMap = useMemo(() => {
    const map = new Map<string, { stakeSol: number; validatorCount: number }>()
    for (const device of devices) {
      map.set(device.pk, {
        stakeSol: device.stake_sol ?? 0,
        validatorCount: device.validator_count ?? 0,
      })
    }
    return map
  }, [devices])

  // Build metro info map for overlay panel (maps metro PK to code/name)
  const metroInfoMap = useMemo(() => {
    const map = new Map<string, { code: string; name: string }>()
    for (const metro of metros) {
      map.set(metro.pk, { code: metro.code, name: metro.name })
    }
    return map
  }, [metros])

  // Build edge traffic map for overlay panel (maps link PK to traffic info)
  const edgeTrafficMap = useMemo(() => {
    const map = new Map<string, { inBps: number; outBps: number; bandwidthBps: number; utilization: number }>()
    for (const link of links) {
      const inBps = link.in_bps ?? 0
      const outBps = link.out_bps ?? 0
      const bandwidthBps = link.bandwidth_bps ?? 0
      const utilization = bandwidthBps > 0 ? Math.max(inBps, outBps) / bandwidthBps : 0
      map.set(link.pk, { inBps, outBps, bandwidthBps, utilization })
    }
    return map
  }, [links])

  // Calculate device positions
  const devicePositions = useMemo(() => {
    const positions = new Map<string, [number, number]>()

    for (const [metroPk, metroDevices] of devicesByMetro) {
      const metro = metroMap.get(metroPk)
      if (!metro) continue

      metroDevices.forEach((device, index) => {
        const pos = calculateDevicePosition(
          metro.latitude,
          metro.longitude,
          index,
          metroDevices.length
        )
        positions.set(device.pk, pos)
      })
    }

    return positions
  }, [devicesByMetro, metroMap])

  // Map style based on theme
  const mapStyle = useMemo(() => createMapStyle(isDark), [isDark])

  // Fit bounds to metros
  const fitBounds = useCallback(() => {
    if (!mapRef.current || metros.length === 0) return

    const lngs = metros.map(m => m.longitude)
    const lats = metros.map(m => m.latitude)
    const bounds: LngLatBoundsLike = [
      [Math.min(...lngs), Math.min(...lats)],
      [Math.max(...lngs), Math.max(...lats)],
    ]
    mapRef.current.fitBounds(bounds, { padding: 50, maxZoom: 5 })
  }, [metros])

  // Fit bounds on initial load
  const initialFitRef = useRef(true)
  useEffect(() => {
    if (initialFitRef.current && metros.length > 0 && mapRef.current) {
      // Wait for map to be ready
      const timer = setTimeout(() => {
        fitBounds()
        initialFitRef.current = false
      }, 100)
      return () => clearTimeout(timer)
    }
  }, [metros, fitBounds])

  // Fetch paths when source and target are set
  useEffect(() => {
    if (!pathModeEnabled || !pathSource || !pathTarget) return

    setPathLoading(true)
    setSelectedPathIndex(0)
    fetchISISPaths(pathSource, pathTarget, 5, pathMode)
      .then(result => {
        setPathsResult(result)
        // Turn off device/link type overlays when path is found to make path visualization clearer
        if (result.paths?.length > 0) {
          if (overlays.deviceType) toggleOverlay('deviceType')
          if (overlays.linkType) toggleOverlay('linkType')
        }
      })
      .catch(err => {
        setPathsResult({ paths: [], from: pathSource, to: pathTarget, error: err.message })
      })
      .finally(() => {
        setPathLoading(false)
      })
  // eslint-disable-next-line react-hooks/exhaustive-deps -- overlays/toggleOverlay are intentionally excluded to avoid re-fetching when overlays change
  }, [pathModeEnabled, pathSource, pathTarget, pathMode])

  // Fetch reverse paths when showReverse is enabled
  useEffect(() => {
    if (!pathModeEnabled || !pathSource || !pathTarget || !showReverse) {
      setReversePathsResult(null)
      return
    }

    setReversePathLoading(true)
    setSelectedReversePathIndex(0)
    fetchISISPaths(pathTarget, pathSource, 5, pathMode)
      .then(result => {
        setReversePathsResult(result)
      })
      .catch(err => {
        setReversePathsResult({ paths: [], from: pathTarget, to: pathSource, error: err.message })
      })
      .finally(() => {
        setReversePathLoading(false)
      })
  }, [pathModeEnabled, pathSource, pathTarget, pathMode, showReverse])

  // Fetch metro paths when source and target metros are set
  const metroPathModeEnabled = mode === 'metro-path'
  useEffect(() => {
    if (!metroPathModeEnabled || !metroPathSource || !metroPathTarget) {
      return
    }

    setMetroPathLoading(true)
    setMetroPathViewMode('aggregate')
    setMetroPathSelectedPairs([])
    fetchMetroDevicePaths(metroPathSource, metroPathTarget, pathMode)
      .then(result => {
        setMetroPathsResult(result)
        // Turn off device/link type overlays when paths are found
        if (result.devicePairs?.length > 0) {
          if (overlays.deviceType) toggleOverlay('deviceType')
          if (overlays.linkType) toggleOverlay('linkType')
        }
      })
      .catch(err => {
        setMetroPathsResult({
          fromMetroPK: metroPathSource,
          fromMetroCode: '',
          toMetroPK: metroPathTarget,
          toMetroCode: '',
          sourceDeviceCount: 0,
          targetDeviceCount: 0,
          totalPairs: 0,
          minHops: 0,
          maxHops: 0,
          minLatencyMs: 0,
          maxLatencyMs: 0,
          avgLatencyMs: 0,
          devicePairs: [],
          error: err.message,
        })
      })
      .finally(() => {
        setMetroPathLoading(false)
      })
  // eslint-disable-next-line react-hooks/exhaustive-deps -- overlays/toggleOverlay are intentionally excluded
  }, [metroPathModeEnabled, metroPathSource, metroPathTarget, pathMode])

  // Clear path when exiting path mode
  useEffect(() => {
    if (!pathModeEnabled) {
      setPathSource(null)
      setPathTarget(null)
      setPathsResult(null)
      setSelectedPathIndex(0)
    }
  }, [pathModeEnabled])

  // Clear metro path state when exiting metro-path mode
  useEffect(() => {
    if (!metroPathModeEnabled) {
      setMetroPathSource(null)
      setMetroPathTarget(null)
      setMetroPathsResult(null)
      setMetroPathSelectedPairs([])
      setMetroPathViewMode('aggregate')
    }
  }, [metroPathModeEnabled])

  // Clear whatif-removal state when exiting mode
  useEffect(() => {
    if (!whatifRemovalMode) {
      setRemovalLink(null)
      setRemovalResult(null)
    }
  }, [whatifRemovalMode])

  // Clear whatif-addition state when exiting mode
  useEffect(() => {
    if (!whatifAdditionMode) {
      setAdditionSource(null)
      setAdditionTarget(null)
      setAdditionResult(null)
    }
  }, [whatifAdditionMode])

  // Clear impact result when exiting mode (impactDevices is cleared by context)
  useEffect(() => {
    if (!impactMode) {
      setImpactResult(null)
    }
  }, [impactMode])

  // Track whether mode params have been restored from URL (used to prevent sync from clearing params on load)
  // Using state instead of ref ensures sync effect re-runs with correct values after restoration
  const [modeParamsRestored, setModeParamsRestored] = useState(false)

  // Sync mode selections to URL for sharing
  useEffect(() => {
    // Don't sync until restoration is complete, otherwise we clear params before they're read
    if (!modeParamsRestored) return

    const params = new URLSearchParams(searchParams)
    let changed = false

    // Helper to set or delete a param
    const setParam = (key: string, value: string | null) => {
      if (value) {
        if (params.get(key) !== value) {
          params.set(key, value)
          changed = true
        }
      } else if (params.has(key)) {
        params.delete(key)
        changed = true
      }
    }

    // In analysis modes, clear the generic selection params to avoid duplication
    const inAnalysisMode = pathModeEnabled || whatifRemovalMode || whatifAdditionMode || impactMode
    if (inAnalysisMode) {
      setParam('type', null)
      setParam('id', null)
    }

    // Path mode params
    setParam('path_source', pathModeEnabled ? pathSource : null)
    setParam('path_target', pathModeEnabled ? pathTarget : null)

    // What-if removal params
    setParam('removal_link', whatifRemovalMode ? removalLink?.linkPK ?? null : null)

    // What-if addition params
    setParam('addition_source', whatifAdditionMode ? additionSource : null)
    setParam('addition_target', whatifAdditionMode ? additionTarget : null)

    // Impact mode params (comma-separated list of device PKs)
    setParam('impact_devices', impactMode && impactDevices.length > 0 ? impactDevices.join(',') : null)

    // Multicast group selection and disabled publishers/subscribers
    setParam('multicast', multicastTreesMode && !!selectedMulticastGroup ? selectedMulticastGroup : null)
    if (multicastTreesMode && selectedMulticastGroup) {
      const detail = multicastGroupDetails.get(selectedMulticastGroup)
      if (detail?.members) {
        const disabledPubs = detail.members
          .filter(m => (m.mode === 'P' || m.mode === 'P+S') && !enabledPublishers.has(m.device_pk))
          .map(m => m.device_pk)
        const disabledSubs = detail.members
          .filter(m => (m.mode === 'S' || m.mode === 'P+S') && !enabledSubscribers.has(m.device_pk))
          .map(m => m.device_pk)
        setParam('mc_pub_off', disabledPubs.length > 0 ? disabledPubs.join(',') : null)
        setParam('mc_sub_off', disabledSubs.length > 0 ? disabledSubs.join(',') : null)
      } else {
        setParam('mc_pub_off', null)
        setParam('mc_sub_off', null)
      }
    } else {
      setParam('mc_pub_off', null)
      setParam('mc_sub_off', null)
    }

    if (changed) {
      setSearchParams(params, { replace: true })
    }
  }, [modeParamsRestored, searchParams, setSearchParams, pathModeEnabled, pathSource, pathTarget, whatifRemovalMode, removalLink, whatifAdditionMode, additionSource, additionTarget, impactMode, impactDevices, multicastTreesMode, selectedMulticastGroup, multicastGroupDetails, enabledPublishers, enabledSubscribers])

  // When entering analysis modes with a device already selected, use it as source
  const prevMapModeRef = useRef<string>(mode)
  useEffect(() => {
    const selectedDevicePK = selectedItem?.type === 'device' ? selectedItem.data.pk : null

    // whatif-addition: use selected device as source
    if (whatifAdditionMode && prevMapModeRef.current !== 'whatif-addition' && selectedDevicePK) {
      setAdditionSource(selectedDevicePK)
    }

    // path mode: use selected device as source
    if (pathModeEnabled && prevMapModeRef.current !== 'path' && selectedDevicePK) {
      setPathSource(selectedDevicePK)
    }

    // impact mode: use selected device for failure analysis (add to impact devices list)
    if (impactMode && prevMapModeRef.current !== 'impact' && selectedDevicePK && !impactDevices.includes(selectedDevicePK)) {
      toggleImpactDevice(selectedDevicePK)
    }

    prevMapModeRef.current = mode
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, whatifAdditionMode, pathModeEnabled, impactMode, selectedItem])

  // Fetch link removal simulation when link is selected
  useEffect(() => {
    if (!removalLink) return

    setRemovalLoading(true)
    fetchSimulateLinkRemoval(removalLink.sourcePK, removalLink.targetPK)
      .then(result => {
        setRemovalResult(result)
      })
      .catch(err => {
        setRemovalResult({
          sourcePK: removalLink.sourcePK,
          sourceCode: '',
          targetPK: removalLink.targetPK,
          targetCode: '',
          disconnectedDevices: [],
          disconnectedCount: 0,
          affectedPaths: [],
          affectedPathCount: 0,
          causesPartition: false,
          error: err.message,
        })
      })
      .finally(() => {
        setRemovalLoading(false)
      })
  }, [removalLink])

  // Fetch link addition simulation when both devices are selected
  useEffect(() => {
    if (!additionSource || !additionTarget) return

    setAdditionLoading(true)
    fetchSimulateLinkAddition(additionSource, additionTarget, additionMetric)
      .then(result => {
        setAdditionResult(result)
      })
      .catch(err => {
        setAdditionResult({
          sourcePK: additionSource,
          sourceCode: '',
          targetPK: additionTarget,
          targetCode: '',
          metric: additionMetric,
          improvedPaths: [],
          improvedPathCount: 0,
          redundancyGains: [],
          redundancyCount: 0,
          error: err.message,
        })
      })
      .finally(() => {
        setAdditionLoading(false)
      })
  }, [additionSource, additionTarget, additionMetric])

  // Analyze failure impact when impactDevices changes
  useEffect(() => {
    if (impactDevices.length === 0) {
      setImpactResult(null)
      return
    }

    setImpactLoading(true)
    fetchWhatIfRemoval(impactDevices, [])
      .then(result => {
        setImpactResult(result)
      })
      .catch(err => {
        setImpactResult({
          items: [],
          totalAffectedPaths: 0,
          totalDisconnected: 0,
          error: err.message,
        })
      })
      .finally(() => {
        setImpactLoading(false)
      })
  }, [impactDevices])

  // Build map of device PKs to path indices for all paths
  const devicePathMap = useMemo(() => {
    const map = new Map<string, number[]>()
    if (!pathsResult?.paths?.length) return map

    pathsResult.paths.forEach((path, pathIndex) => {
      path.path.forEach(hop => {
        const existing = map.get(hop.devicePK) || []
        if (!existing.includes(pathIndex)) {
          existing.push(pathIndex)
        }
        map.set(hop.devicePK, existing)
      })
    })
    return map
  }, [pathsResult])

  // Build map of link PKs to path indices for all paths
  const linkPathMap = useMemo(() => {
    const map = new Map<string, number[]>()
    if (!pathsResult?.paths?.length) return map

    pathsResult.paths.forEach((singlePath, pathIndex) => {
      // For each consecutive pair in the path, find the link between them
      for (let i = 0; i < singlePath.path.length - 1; i++) {
        const fromPK = singlePath.path[i].devicePK
        const toPK = singlePath.path[i + 1].devicePK

        // Find link that connects these two devices
        for (const link of links) {
          if ((link.side_a_pk === fromPK && link.side_z_pk === toPK) ||
              (link.side_a_pk === toPK && link.side_z_pk === fromPK)) {
            const existing = map.get(link.pk) || []
            if (!existing.includes(pathIndex)) {
              existing.push(pathIndex)
            }
            map.set(link.pk, existing)
            break
          }
        }
      }
    })
    return map
  }, [pathsResult, links])

  // Build map of device PKs to path indices for metro paths
  const metroDevicePathMap = useMemo(() => {
    const map = new Map<string, number[]>()
    if (!metroPathsResult?.devicePairs?.length) return map

    metroPathsResult.devicePairs.forEach((pair, pairIndex) => {
      pair.bestPath?.path?.forEach(hop => {
        const existing = map.get(hop.devicePK) || []
        if (!existing.includes(pairIndex)) {
          existing.push(pairIndex)
        }
        map.set(hop.devicePK, existing)
      })
    })
    return map
  }, [metroPathsResult])

  // Build map of link PKs to path indices for metro paths
  const metroLinkPathMap = useMemo(() => {
    const map = new Map<string, number[]>()
    if (!metroPathsResult?.devicePairs?.length) return map

    metroPathsResult.devicePairs.forEach((pair, pairIndex) => {
      const path = pair.bestPath?.path
      if (!path?.length) return

      for (let i = 0; i < path.length - 1; i++) {
        const fromPK = path[i].devicePK
        const toPK = path[i + 1].devicePK

        for (const link of links) {
          if ((link.side_a_pk === fromPK && link.side_z_pk === toPK) ||
              (link.side_a_pk === toPK && link.side_z_pk === fromPK)) {
            const existing = map.get(link.pk) || []
            if (!existing.includes(pairIndex)) {
              existing.push(pairIndex)
            }
            map.set(link.pk, existing)
            break
          }
        }
      }
    })
    return map
  }, [metroPathsResult, links])

  // Build per-publisher tree segments with device PKs and positions for drawing offset parallel lines.
  // Each publisher's tree is an array of directed segments with from/to device PKs and positions.
  const multicastPublisherPaths = useMemo(() => {
    const result: Array<{ publisherPK: string; segments: Array<{ fromPK: string; toPK: string; from: [number, number]; to: [number, number] }> }> = []
    if (!multicastTreesMode || !selectedMulticastGroup) return result

    const publisherSegments = new Map<string, Map<string, { fromPK: string; toPK: string; from: [number, number]; to: [number, number] }>>()

    const treeData = multicastTreePaths.get(selectedMulticastGroup)
    if (treeData?.paths?.length) {
      treeData.paths.forEach(treePath => {
        const path = treePath.path
        const publisherPK = treePath.publisherDevicePK
        const subscriberPK = treePath.subscriberDevicePK
        if (!path?.length) return
        if (!enabledPublishers.has(publisherPK) || !enabledSubscribers.has(subscriberPK)) return

        if (!publisherSegments.has(publisherPK)) publisherSegments.set(publisherPK, new Map())
        const segs = publisherSegments.get(publisherPK)!

        for (let i = 0; i < path.length - 1; i++) {
          const fromPK = path[i].devicePK
          const toPK = path[i + 1].devicePK
          const key = `${fromPK}|${toPK}`
          if (segs.has(key)) continue

          const fromPos = devicePositions.get(fromPK)
          const toPos = devicePositions.get(toPK)
          if (fromPos && toPos) {
            segs.set(key, { fromPK, toPK, from: fromPos, to: toPos })
          }
        }
      })
    }

    for (const [publisherPK, segs] of publisherSegments) {
      const segments = Array.from(segs.values())
      if (segments.length > 0) result.push({ publisherPK, segments })
    }

    return result
  }, [multicastTreesMode, selectedMulticastGroup, multicastTreePaths, enabledPublishers, enabledSubscribers, devicePositions])

  // Map from canonical segment key (sorted device PKs) -> ordered publisher PKs (for offset calculation)
  const multicastSegmentPublishers = useMemo(() => {
    const map = new Map<string, string[]>()
    if (!multicastTreesMode || !selectedMulticastGroup) return map

    const treeData = multicastTreePaths.get(selectedMulticastGroup)
    if (treeData?.paths?.length) {
      treeData.paths.forEach(treePath => {
        const path = treePath.path
        if (!path?.length) return
        if (!enabledPublishers.has(treePath.publisherDevicePK) || !enabledSubscribers.has(treePath.subscriberDevicePK)) return

        for (let i = 0; i < path.length - 1; i++) {
          const canonicalKey = [path[i].devicePK, path[i + 1].devicePK].sort().join('|')
          if (!map.has(canonicalKey)) map.set(canonicalKey, [])
          const pubs = map.get(canonicalKey)!
          if (!pubs.includes(treePath.publisherDevicePK)) pubs.push(treePath.publisherDevicePK)
        }
      })
    }
    return map
  }, [multicastTreesMode, selectedMulticastGroup, multicastTreePaths, enabledPublishers, enabledSubscribers])

  // Set of device PKs that are in any multicast tree path (for dimming non-tree links)
  const multicastTreeDevicePKs = useMemo(() => {
    const set = new Set<string>()
    if (!multicastTreesMode || !selectedMulticastGroup) return set
    const treeData = multicastTreePaths.get(selectedMulticastGroup)
    if (treeData?.paths?.length) {
      treeData.paths.forEach(treePath => {
        if (!treePath.path?.length) return
        if (!enabledPublishers.has(treePath.publisherDevicePK) || !enabledSubscribers.has(treePath.subscriberDevicePK)) return
        treePath.path.forEach(hop => set.add(hop.devicePK))
      })
    }
    return set
  }, [multicastTreesMode, selectedMulticastGroup, multicastTreePaths, enabledPublishers, enabledSubscribers])


  // Set of link PKs that are in any multicast tree (for dimming)
  const multicastTreeLinkPKs = useMemo(() => {
    const set = new Set<string>()
    if (!multicastTreesMode || !selectedMulticastGroup) return set
    const treeData = multicastTreePaths.get(selectedMulticastGroup)
    if (treeData?.paths?.length) {
      treeData.paths.forEach(treePath => {
        const path = treePath.path
        if (!path?.length) return
        if (!enabledPublishers.has(treePath.publisherDevicePK) || !enabledSubscribers.has(treePath.subscriberDevicePK)) return
        for (let i = 0; i < path.length - 1; i++) {
          const fromPK = path[i].devicePK
          const toPK = path[i + 1].devicePK
          for (const link of links) {
            if ((link.side_a_pk === fromPK && link.side_z_pk === toPK) ||
                (link.side_a_pk === toPK && link.side_z_pk === fromPK)) {
              set.add(link.pk)
              break
            }
          }
        }
      })
    }
    return set
  }, [multicastTreesMode, selectedMulticastGroup, multicastTreePaths, links, enabledPublishers, enabledSubscribers])

  // Build ordered list of unique publisher PKs for consistent color assignment
  const multicastPublisherColorMap = useMemo(() => {
    const map = new Map<string, number>()
    if (!multicastTreesMode || !selectedMulticastGroup) return map

    let colorIndex = 0
    if (selectedMulticastGroup) {
      const code = selectedMulticastGroup
      const detail = multicastGroupDetails.get(code)
      if (detail?.members) {
        detail.members
          .filter(m => m.mode === 'P' || m.mode === 'P+S')
          .forEach(m => {
            if (!map.has(m.device_pk)) {
              map.set(m.device_pk, colorIndex++)
            }
          })
      }
    }
    return map
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails])

  // Map device_pk -> role color for validators on multicast member devices
  // Respects enabled state: P+S devices only get publisher color when enabled as publisher
  const multicastDeviceRoleColorMap = useMemo(() => {
    const map = new Map<string, string>()
    if (!multicastTreesMode || !selectedMulticastGroup) return map
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return map
    for (const m of detail.members) {
      const isPub = (m.mode === 'P' || m.mode === 'P+S') && enabledPublishers.has(m.device_pk)
      const isSub = (m.mode === 'S' || m.mode === 'P+S') && enabledSubscribers.has(m.device_pk)
      if (isPub) {
        const colorIndex = multicastPublisherColorMap.get(m.device_pk) ?? 0
        const c = MULTICAST_PUBLISHER_COLORS[colorIndex % MULTICAST_PUBLISHER_COLORS.length]
        map.set(m.device_pk, isDark ? c.dark : c.light)
      } else if (isSub) {
        map.set(m.device_pk, '#ef4444') // red for subscriber
      }
    }
    return map
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails, multicastPublisherColorMap, enabledPublishers, enabledSubscribers, isDark])

  // Build set of publisher and subscriber device PKs for multicast trees (filtered by enabled)
  const multicastPublisherDevices = useMemo(() => {
    const set = new Set<string>()
    if (!multicastTreesMode) return set
    if (selectedMulticastGroup) {
      const code = selectedMulticastGroup
      const detail = multicastGroupDetails.get(code)
      if (detail?.members) {
        detail.members
          .filter(m => m.mode === 'P' || m.mode === 'P+S')
          .filter(m => enabledPublishers.has(m.device_pk))
          .forEach(m => set.add(m.device_pk))
      }
    }
    return set
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails, enabledPublishers])

  const multicastSubscriberDevices = useMemo(() => {
    const set = new Set<string>()
    if (!multicastTreesMode) return set
    if (selectedMulticastGroup) {
      const code = selectedMulticastGroup
      const detail = multicastGroupDetails.get(code)
      if (detail?.members) {
        detail.members
          .filter(m => m.mode === 'S' || m.mode === 'P+S')
          .filter(m => enabledSubscribers.has(m.device_pk))
          .forEach(m => set.add(m.device_pk))
      }
    }
    return set
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails, enabledSubscribers])

  // Validators on multicast publisher/subscriber devices (auto-shown when tree is active)
  // Deduplicated to 1 per device — avoids showing many markers for multi-validator devices
  const multicastTreeValidators = useMemo(() => {
    if (!multicastTreesMode || !selectedMulticastGroup) return []
    if (multicastPublisherDevices.size === 0 && multicastSubscriberDevices.size === 0) return []
    const seen = new Set<string>()
    return validators.filter(v => {
      if (seen.has(v.device_pk)) return false
      if (!multicastPublisherDevices.has(v.device_pk) && !multicastSubscriberDevices.has(v.device_pk)) return false
      seen.add(v.device_pk)
      return true
    })
  }, [multicastTreesMode, selectedMulticastGroup, multicastPublisherDevices, multicastSubscriberDevices, validators])

  // Validators to render: either all (overlay toggle) or tree-filtered (multicast mode, if toggled on)
  const visibleValidators = useMemo(() => {
    // When multicast tree is active, show only tree-relevant validators (unless tree validators toggled off)
    if (multicastTreesMode && selectedMulticastGroup && multicastTreeValidators.length > 0) {
      return showTreeValidators ? multicastTreeValidators : []
    }
    if (showValidators && !pathModeEnabled) return validators
    return []
  }, [multicastTreesMode, selectedMulticastGroup, showValidators, pathModeEnabled, validators, showTreeValidators, multicastTreeValidators])

  // Count publishers and subscribers per device for multicast hover tooltip (filtered by enabled)
  const multicastPublisherCount = useMemo(() => {
    const map = new Map<string, number>()
    if (!multicastTreesMode) return map
    if (selectedMulticastGroup) {
      const code = selectedMulticastGroup
      const detail = multicastGroupDetails.get(code)
      if (detail?.members) {
        detail.members
          .filter(m => m.mode === 'P' || m.mode === 'P+S')
          .filter(m => enabledPublishers.has(m.device_pk))
          .forEach(m => map.set(m.device_pk, (map.get(m.device_pk) || 0) + 1))
      }
    }
    return map
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails, enabledPublishers])

  const multicastSubscriberCount = useMemo(() => {
    const map = new Map<string, number>()
    if (!multicastTreesMode) return map
    if (selectedMulticastGroup) {
      const code = selectedMulticastGroup
      const detail = multicastGroupDetails.get(code)
      if (detail?.members) {
        detail.members
          .filter(m => m.mode === 'S' || m.mode === 'P+S')
          .filter(m => enabledSubscribers.has(m.device_pk))
          .forEach(m => map.set(m.device_pk, (map.get(m.device_pk) || 0) + 1))
      }
    }
    return map
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails, enabledSubscribers])

  // Handle device click for path finding
  const handlePathDeviceClick = useCallback((devicePK: string) => {
    if (!pathSource) {
      setPathSource(devicePK)
    } else if (!pathTarget && devicePK !== pathSource) {
      setPathTarget(devicePK)
    } else {
      // Reset and start new path
      setPathSource(devicePK)
      setPathTarget(null)
      setPathsResult(null)
      setSelectedPathIndex(0)
    }
  }, [pathSource, pathTarget])

  const clearPath = useCallback(() => {
    setPathSource(null)
    setPathTarget(null)
    setPathsResult(null)
    setSelectedPathIndex(0)
    setShowReverse(false)
    setReversePathsResult(null)
    setSelectedReversePathIndex(0)
  }, [])

  // Criticality colors
  const criticalityColors = {
    critical: '#ef4444',    // red
    important: '#eab308',   // yellow
    redundant: '#22c55e',   // green
  }

  // Build set of disconnected device PKs from removal result
  const disconnectedDevicePKs = useMemo(() => {
    const set = new Set<string>()
    if (removalResult?.disconnectedDevices) {
      removalResult.disconnectedDevices.forEach(d => set.add(d.pk))
    }
    return set
  }, [removalResult])

  // GeoJSON for link lines
  const linkGeoJson = useMemo(() => {
    // When metro clustering mode with collapsed metros, track inter-metro edges
    const interMetroEdges = new Map<string, { count: number; totalLatency: number; latencyCount: number }>()

    const features = links.map(link => {
      const deviceA = deviceMap.get(link.side_a_pk)
      const deviceZ = deviceMap.get(link.side_z_pk)
      const metroAPK = deviceA?.metro_pk
      const metroZPK = deviceZ?.metro_pk

      // Handle collapsed metros
      if (metroClusteringMode && metroAPK && metroZPK) {
        const aCollapsed = collapsedMetros.has(metroAPK)
        const zCollapsed = collapsedMetros.has(metroZPK)

        // Skip intra-metro links when the metro is collapsed
        if (metroAPK === metroZPK && aCollapsed) {
          return null
        }

        // Track inter-metro edges for aggregation
        if (aCollapsed || zCollapsed) {
          if (metroAPK !== metroZPK) {
            // This is an inter-metro link with at least one collapsed end
            const edgeKey = [metroAPK, metroZPK].sort().join('|')
            const existing = interMetroEdges.get(edgeKey) || { count: 0, totalLatency: 0, latencyCount: 0 }
            existing.count++
            if (link.latency_us > 0) {
              existing.totalLatency += link.latency_us
              existing.latencyCount++
            }
            interMetroEdges.set(edgeKey, existing)
          }
          // Skip individual link rendering if either end is collapsed
          if (aCollapsed && zCollapsed) {
            return null
          }
        }
      }

      const startPos = devicePositions.get(link.side_a_pk)
      const endPos = devicePositions.get(link.side_z_pk)

      if (!startPos || !endPos) return null

      const isHovered = hoveredLink?.pk === link.pk
      const isSelected = selectedItem?.type === 'link' && selectedItem.data.pk === link.pk
      const linkPathIndices = linkPathMap.get(link.pk)
      const isInAnyPath = linkPathIndices && linkPathIndices.length > 0
      const isInSelectedPath = linkPathIndices?.includes(selectedPathIndex)
      // Metro path mode
      const metroLinkPathIndices = metroLinkPathMap.get(link.pk)
      const isInAnyMetroPath = metroLinkPathIndices && metroLinkPathIndices.length > 0
      const isInSelectedMetroPath = metroPathSelectedPairs.length > 0 && metroLinkPathIndices?.some(idx => metroPathSelectedPairs.includes(idx))
      // Multicast tree mode - check if link is in any tree path (for dimming only)
      const isInAnyMulticastTree = multicastTreeLinkPKs.has(link.pk)
      const criticality = linkCriticalityMap.get(link.pk)
      const isRemovedLink = removalLink?.linkPK === link.pk

      // Determine display color based on mode
      // Default to cyan (dark) / blue (light) when no overlay is active (matches globe view)
      let displayColor = isDark ? 'rgba(0,255,204,0.7)' : 'rgba(59,130,246,0.7)'
      // Bandwidth sizing when overlay is active; uniform weight otherwise
      const defaultWeight = 1.5
      let displayWeight = bandwidthMode ? getBandwidthWeight(link.bandwidth_bps) : defaultWeight
      let displayOpacity = 0.7
      let useDash = false

      if (whatifRemovalMode && isRemovedLink) {
        // Whatif-removal mode: highlight removed link in red dashed
        displayColor = '#ef4444'
        displayWeight = defaultWeight + 3
        displayOpacity = 0.6
        useDash = true
      } else if (linkHealthMode) {
        // Link health mode: color by SLA status (preserves bandwidth weight if active)
        const slaInfo = linkSlaStatus.get(link.pk)
        if (slaInfo) {
          switch (slaInfo.status) {
            case 'healthy':
              displayColor = '#22c55e' // green
              if (!bandwidthMode) displayWeight = defaultWeight + 1
              displayOpacity = 0.9
              break
            case 'warning':
              displayColor = '#eab308' // yellow
              if (!bandwidthMode) displayWeight = defaultWeight + 1
              displayOpacity = 1
              break
            case 'critical':
              displayColor = '#ef4444' // red
              if (!bandwidthMode) displayWeight = defaultWeight + 2
              displayOpacity = 1
              break
            default:
              displayColor = isDark ? '#6b7280' : '#9ca3af' // gray
              displayOpacity = 0.5
          }
        } else {
          displayColor = isDark ? '#6b7280' : '#9ca3af' // gray for no data
          displayOpacity = 0.5
        }
      } else if (trafficFlowMode) {
        // Traffic flow mode: color by utilization (preserves bandwidth weight if active)
        const trafficStyle = getTrafficColor(link)
        displayColor = trafficStyle.color
        if (!bandwidthMode) displayWeight = trafficStyle.weight
        displayOpacity = trafficStyle.opacity
      } else if (contributorLinksMode && link.contributor_pk) {
        // Contributor links mode: color by contributor (preserves bandwidth weight if active)
        const contributorIndex = contributorIndexMap.get(link.contributor_pk) ?? 0
        displayColor = CONTRIBUTOR_COLORS[contributorIndex % CONTRIBUTOR_COLORS.length]
        if (!bandwidthMode) displayWeight = defaultWeight + 1
        displayOpacity = 0.9
      } else if (contributorLinksMode && !link.contributor_pk) {
        // Contributor links mode but no contributor - dim the link
        displayColor = isDark ? '#6b7280' : '#9ca3af'
        displayOpacity = 0.3
      } else if (linkTypeMode) {
        // Link type mode: color by link type (default overlay)
        const linkType = link.link_type || 'default'
        const colors = LINK_TYPE_COLORS[linkType] || LINK_TYPE_COLORS.default
        displayColor = isDark ? colors.dark : colors.light
        if (!bandwidthMode) displayWeight = linkType === 'WAN' ? defaultWeight + 1 : defaultWeight
        displayOpacity = 0.8
      } else if (criticalityOverlayEnabled && criticality) {
        // Criticality mode: color by criticality level (preserves bandwidth weight if active)
        displayColor = criticalityColors[criticality]
        if (!bandwidthMode) displayWeight = criticality === 'critical' ? defaultWeight + 3 : criticality === 'important' ? defaultWeight + 2 : defaultWeight + 1
        displayOpacity = 1
        useDash = true
      } else if (isisHealthMode) {
        // ISIS overlay: color by health status, thickness by metric
        const healthKey = `${link.side_a_pk}|${link.side_z_pk}`
        const healthStatus = edgeHealthStatus.get(healthKey)

        // Get metric-based width
        const metric = link.latency_us ?? 0
        if (metric <= 0) {
          displayWeight = 1
        } else if (metric <= 1000) {
          displayWeight = 4
        } else if (metric <= 5000) {
          displayWeight = 3
        } else {
          displayWeight = 2
        }

        // Color by health status
        if (healthStatus === 'missing') {
          displayColor = '#ef4444' // red
          useDash = true
          displayOpacity = 1
        } else if (healthStatus === 'extra') {
          displayColor = '#f59e0b' // amber
          displayOpacity = 1
        } else if (healthStatus === 'mismatch') {
          displayColor = '#eab308' // yellow
          displayOpacity = 1
        } else {
          // Default to matched (green)
          displayColor = '#22c55e'
          displayOpacity = 0.8
        }
      } else if (isInSelectedPath && linkPathIndices) {
        // Use the selected path's color
        displayColor = PATH_COLORS[selectedPathIndex % PATH_COLORS.length]
        displayWeight = defaultWeight + 3
        displayOpacity = 1
      } else if (isInAnyPath && linkPathIndices) {
        // In another path but not selected - use first path's color but dimmed
        const firstPathIndex = linkPathIndices[0]
        displayColor = PATH_COLORS[firstPathIndex % PATH_COLORS.length]
        displayWeight = defaultWeight + 1
        displayOpacity = 0.4
      } else if (metroPathModeEnabled && isInSelectedMetroPath && metroLinkPathIndices) {
        // Metro path: selected pair's path
        displayColor = PATH_COLORS[0]
        displayWeight = defaultWeight + 3
        displayOpacity = 1
      } else if (metroPathModeEnabled && isInAnyMetroPath && metroLinkPathIndices) {
        // Metro path: in any path but not selected - show dimmed
        const firstPairIndex = metroLinkPathIndices[0]
        displayColor = PATH_COLORS[firstPairIndex % PATH_COLORS.length]
        displayWeight = defaultWeight + 1
        displayOpacity = 0.3
      } else if (isSelected) {
        displayColor = '#3b82f6' // blue - selection color
        displayWeight = defaultWeight + 3
        displayOpacity = 1
      } else if (isHovered) {
        displayColor = hoverHighlight
        displayWeight = defaultWeight + 2
        displayOpacity = 1
      }

      // Dim non-highlighted links when multicast overlay is active and dimming enabled
      if (multicastTreesMode && dimOtherLinks && !isInAnyMulticastTree && !isSelected && !isHovered) {
        displayOpacity = 0.08
      }

      return {
        type: 'Feature' as const,
        properties: {
          pk: link.pk,
          code: link.code,
          color: displayColor,
          weight: displayWeight,
          opacity: displayOpacity,
          useDash,
          isSelected: isSelected ? 1 : 0,
          latencyUs: link.latency_us ?? 0,
        },
        geometry: {
          type: 'LineString' as const,
          coordinates: calculateCurvedPath(startPos, endPos),
        },
      }
    }).filter((f): f is NonNullable<typeof f> => f !== null)

    // Add inter-metro edges for collapsed metros
    if (metroClusteringMode && collapsedMetros.size > 0) {
      for (const [edgeKey, data] of interMetroEdges) {
        const [metroAPK, metroZPK] = edgeKey.split('|')
        const metroA = metroMap.get(metroAPK)
        const metroZ = metroMap.get(metroZPK)
        if (!metroA || !metroZ) continue

        // Only show inter-metro edge if at least one end is collapsed
        if (!collapsedMetros.has(metroAPK) && !collapsedMetros.has(metroZPK)) continue

        const avgLatencyMs: string = data.latencyCount > 0
          ? ((data.totalLatency / data.latencyCount) / 1000).toFixed(2)
          : 'N/A';

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (features as any[]).push({
          type: 'Feature' as const,
          properties: {
            pk: `inter-metro-${edgeKey}`,
            code: `${metroA.code} ↔ ${metroZ.code}`,
            color: isDark ? '#94a3b8' : '#64748b',
            weight: 4,
            opacity: 0.8,
            useDash: true,
            isInterMetro: true,
            linkCount: data.count,
            avgLatencyMs,
          },
          geometry: {
            type: 'LineString' as const,
            coordinates: calculateCurvedPath(
              [metroA.longitude, metroA.latitude],
              [metroZ.longitude, metroZ.latitude]
            ),
          },
        })
      }
    }

    return {
      type: 'FeatureCollection' as const,
      features,
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [links, devicePositions, isDark, hoveredLink, selectedItem, hoverHighlight, linkPathMap, selectedPathIndex, criticalityOverlayEnabled, linkCriticalityMap, whatifRemovalMode, removalLink, linkHealthMode, linkSlaStatus, trafficFlowMode, getTrafficColor, metroClusteringMode, collapsedMetros, deviceMap, metroMap, contributorLinksMode, contributorIndexMap, bandwidthMode, isisHealthMode, edgeHealthStatus, linkTypeMode, metroPathModeEnabled, metroLinkPathMap, metroPathSelectedPairs, multicastTreesMode, multicastTreeLinkPKs, dimOtherLinks])

  // GeoJSON for validator links (connecting lines)
  const validatorLinksGeoJson = useMemo(() => {
    if (visibleValidators.length === 0) return { type: 'FeatureCollection' as const, features: [] }

    const defaultLinkColor = '#7c3aed'
    const features = visibleValidators.map(validator => {
      const devicePos = devicePositions.get(validator.device_pk)
      if (!devicePos) return null

      const isHovered = hoveredValidator?.votePubkey === validator.vote_pubkey
      const roleColor = multicastDeviceRoleColorMap.get(validator.device_pk)

      return {
        type: 'Feature' as const,
        properties: {
          votePubkey: validator.vote_pubkey,
          color: isHovered ? hoverHighlight : roleColor || defaultLinkColor,
          weight: isHovered ? 2 : roleColor ? 1.5 : 1,
          opacity: isHovered ? 0.9 : roleColor ? 0.6 : 0.4,
        },
        geometry: {
          type: 'LineString' as const,
          coordinates: [
            [validator.longitude, validator.latitude],
            devicePos,
          ],
        },
      }
    }).filter((f): f is NonNullable<typeof f> => f !== null)

    return {
      type: 'FeatureCollection' as const,
      features,
    }
  }, [visibleValidators, devicePositions, isDark, hoveredValidator, hoverHighlight, multicastDeviceRoleColorMap])

  // GeoJSON for per-publisher multicast tree lines (static, with offset curves)
  const multicastTreeGeoJson = useMemo(() => {
    if (!multicastTreesMode || !selectedMulticastGroup || multicastPublisherPaths.length === 0) {
      return { type: 'FeatureCollection' as const, features: [] }
    }
    const features: GeoJSON.Feature<GeoJSON.LineString>[] = []
    for (const { publisherPK, segments } of multicastPublisherPaths) {
      const colorIndex = multicastPublisherColorMap.get(publisherPK) ?? 0
      const mc = MULTICAST_PUBLISHER_COLORS[colorIndex % MULTICAST_PUBLISHER_COLORS.length]
      const color = isDark ? mc.dark : mc.light

      for (const seg of segments) {
        const canonicalKey = [seg.fromPK, seg.toPK].sort().join('|')
        const pubs = multicastSegmentPublishers.get(canonicalKey) ?? [publisherPK]
        const offset = getPublisherOffset(pubs.indexOf(publisherPK), pubs.length)

        features.push({
          type: 'Feature',
          properties: {
            pk: `mc-${publisherPK}-${seg.fromPK}-${seg.toPK}`,
            color,
            weight: 3.5,
            opacity: 0.9,
          },
          geometry: {
            type: 'LineString',
            coordinates: calculateCurvedPath(seg.from, seg.to, offset),
          },
        })
      }
    }
    return { type: 'FeatureCollection' as const, features }
  }, [multicastTreesMode, selectedMulticastGroup, multicastPublisherPaths, multicastSegmentPublishers, multicastPublisherColorMap, isDark])

  // GeoJSON for animated multicast tree links overlay (per-publisher offset dots)
  const multicastAnimatedGeoJson = useMemo(() => {
    if (!multicastTreesMode || !animateFlow || !selectedMulticastGroup || multicastPublisherPaths.length === 0) {
      return { type: 'FeatureCollection' as const, features: [] }
    }
    const features: GeoJSON.Feature<GeoJSON.LineString>[] = []
    for (const { publisherPK, segments } of multicastPublisherPaths) {
      const colorIndex = multicastPublisherColorMap.get(publisherPK) ?? 0
      const mc = MULTICAST_PUBLISHER_COLORS[colorIndex % MULTICAST_PUBLISHER_COLORS.length]
      const color = isDark ? mc.dark : mc.light

      for (const seg of segments) {
        const canonicalKey = [seg.fromPK, seg.toPK].sort().join('|')
        const pubs = multicastSegmentPublishers.get(canonicalKey) ?? [publisherPK]
        const offset = getPublisherOffset(pubs.indexOf(publisherPK), pubs.length)

        features.push({
          type: 'Feature',
          properties: {
            pk: `mc-anim-${publisherPK}-${seg.fromPK}-${seg.toPK}`,
            color,
            weight: 3.5,
          },
          geometry: {
            type: 'LineString',
            coordinates: calculateCurvedPath(seg.from, seg.to, offset),
          },
        })
      }
    }
    // Add validator-to-device flow lines (only for pub/sub validators shown on tree)
    if (showTreeValidators) {
      for (const v of multicastTreeValidators) {
        const devicePos = devicePositions.get(v.device_pk)
        if (!devicePos) continue
        const roleColor = multicastDeviceRoleColorMap.get(v.device_pk)
        const color = roleColor || (isDark ? '#a855f7' : '#7c3aed')
        const isPub = multicastPublisherDevices.has(v.device_pk)
        // Publishers: flow from validator → device; Subscribers: flow from device → validator
        const from: [number, number] = isPub ? [v.longitude, v.latitude] : devicePos
        const to: [number, number] = isPub ? devicePos : [v.longitude, v.latitude]
        features.push({
          type: 'Feature',
          properties: {
            pk: `mc-val-${v.vote_pubkey}`,
            color,
            weight: 3,
          },
          geometry: {
            type: 'LineString',
            coordinates: [from, to],
          },
        })
      }
    }

    return { type: 'FeatureCollection' as const, features }
  }, [multicastTreesMode, animateFlow, selectedMulticastGroup, multicastPublisherPaths, multicastSegmentPublishers, multicastPublisherColorMap, isDark, showTreeValidators, multicastTreeValidators, devicePositions, multicastDeviceRoleColorMap, multicastPublisherDevices])

  // Stabilize the GeoJSON reference so the <Source> doesn't get a new object on
  // every react-query refetch (which causes MapLibre to re-process and flicker).
  // Only produce a new reference when the actual set of feature PKs changes.
  const animatedFeatureKey = useMemo(() => {
    return multicastAnimatedGeoJson.features.map(f => f.properties?.pk).join(',')
  }, [multicastAnimatedGeoJson])

  const stableAnimatedGeoJson = useMemo(() => {
    return multicastAnimatedGeoJson
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [animatedFeatureKey])

  // Animated dots flowing along multicast tree paths (publisher → subscriber).
  // Managed imperatively: adds a GeoJSON point source + circle layer and updates
  // dot positions via setData() in a requestAnimationFrame loop.
  const animatedDotsRef = useRef<GeoJSON.FeatureCollection>({ type: 'FeatureCollection', features: [] })

  useEffect(() => {
    if (!multicastTreesMode || !animateFlow || !linkAnimating || stableAnimatedGeoJson.features.length === 0) return
    const map = mapRef.current?.getMap()
    if (!map) return

    const sourceId = 'multicast-flow-dots'
    const layerId = 'multicast-flow-dots-layer'
    const DOTS_PER_LINK = 3
    const SPEED = 0.0003 // full traversal per ms (~3.3s per link)

    // Add source + layer imperatively (avoids React prop reconciliation issues)
    const addLayer = () => {
      if (map.getSource(sourceId)) return
      map.addSource(sourceId, {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      })
      map.addLayer({
        id: layerId,
        type: 'circle',
        source: sourceId,
        paint: {
          'circle-radius': 4,
          'circle-color': ['get', 'color'],
          'circle-opacity': 0.95,
          'circle-stroke-width': 1,
          'circle-stroke-color': 'rgba(0,0,0,0.3)',
        },
      })
    }

    // Build features list from current animated GeoJSON
    const pathFeatures = stableAnimatedGeoJson.features as GeoJSON.Feature<GeoJSON.LineString>[]

    let frameId: number
    const tick = (timestamp: number) => {
      // Re-add source/layer if removed (react-map-gl style reconciliation can
      // destroy imperatively added layers during its first render pass).
      if (!map.getSource(sourceId) && map.isStyleLoaded()) {
        addLayer()
      }

      const t = (timestamp * SPEED) % 1

      const points: GeoJSON.Feature<GeoJSON.Point>[] = []
      for (const feature of pathFeatures) {
        const coords = feature.geometry.coordinates as [number, number][]
        const color = (feature.properties as Record<string, unknown>)?.color ?? '#a855f7'

        for (let i = 0; i < DOTS_PER_LINK; i++) {
          const dotT = (t + i / DOTS_PER_LINK) % 1
          const pos = interpolateAlongPath(coords, dotT)
          points.push({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: pos },
            properties: { color },
          })
        }
      }

      animatedDotsRef.current = { type: 'FeatureCollection', features: points }
      try {
        const src = map.getSource(sourceId)
        if (src && 'setData' in src) {
          (src as any).setData(animatedDotsRef.current)
        }
      } catch {
        // Source may be removed during cleanup
      }
      frameId = requestAnimationFrame(tick)
    }

    frameId = requestAnimationFrame(tick)
    return () => {
      cancelAnimationFrame(frameId)
      try {
        if (map.getLayer(layerId)) map.removeLayer(layerId)
        if (map.getSource(sourceId)) map.removeSource(sourceId)
      } catch {
        // Map may already be destroyed
      }
    }
  }, [multicastTreesMode, animateFlow, linkAnimating, stableAnimatedGeoJson])

  // Keep a ref to linkGeoJson so the rAF loop always reads the latest value
  const linkGeoJsonRef = useRef(linkGeoJson)
  linkGeoJsonRef.current = linkGeoJson

  // Animate flowing dots along links (matches globe animated arcs, works with all overlays)
  useEffect(() => {
    const map = mapRef.current?.getMap()
    if (!map || !mapReady) return

    const sourceId = 'link-flow-dots'
    const layerId = 'link-flow-dots-layer'
    const DOTS_PER_LINK = 2

    // Per-link cycle duration based on latency (matches globe arcAnimateTime):
    // low latency → faster dots, high latency → slower dots
    // Returns cycle time in ms: 600ms (1µs) to 3000ms (200ms+)
    const getCycleDuration = (latencyUs: number): number => {
      const latencyMs = latencyUs / 1000
      const clamped = Math.max(1, Math.min(200, latencyMs))
      return 600 + (Math.log(clamped) / Math.log(200)) * 2400
    }

    const addLayer = () => {
      if (map.getSource(sourceId)) return
      map.addSource(sourceId, {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      })
      map.addLayer({
        id: layerId,
        type: 'circle',
        source: sourceId,
        paint: {
          'circle-radius': 2.5,
          'circle-color': ['get', 'color'],
          'circle-opacity': 0.8,
        },
      })
    }

    const removeLayer = () => {
      try {
        if (map.getLayer(layerId)) map.removeLayer(layerId)
        if (map.getSource(sourceId)) map.removeSource(sourceId)
      } catch {
        // Map may already be destroyed
      }
    }

    if (!linkAnimating) {
      // Ensure layer is removed when animation is off
      removeLayer()
      return
    }

    // Add layer immediately at setup
    if (map.isStyleLoaded()) {
      addLayer()
    }

    let frameId: number
    const tick = (timestamp: number) => {
      // Re-add if react-map-gl removed it during style reconciliation
      if (!map.getSource(sourceId)) {
        if (map.isStyleLoaded()) addLayer()
        frameId = requestAnimationFrame(tick)
        return
      }

      const features = linkGeoJsonRef.current.features as GeoJSON.Feature<GeoJSON.LineString>[]
      const points: GeoJSON.Feature<GeoJSON.Point>[] = []

      for (const feature of features) {
        const props = feature.properties as Record<string, unknown>
        // Skip heavily dimmed links (e.g. multicast dim-other-links)
        if (typeof props?.opacity === 'number' && props.opacity < 0.2) continue

        const color = (props?.color as string) ?? (isDark ? '#00ffcc' : '#3b82f6')
        const latencyUs = (props?.latencyUs as number) ?? 0
        const cycleDuration = getCycleDuration(latencyUs)
        const t = (timestamp % cycleDuration) / cycleDuration

        const coords = feature.geometry.coordinates as [number, number][]
        for (let i = 0; i < DOTS_PER_LINK; i++) {
          const dotT = (t + i / DOTS_PER_LINK) % 1
          const pos = interpolateAlongPath(coords, dotT)
          points.push({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: pos },
            properties: { color },
          })
        }
      }

      try {
        const src = map.getSource(sourceId)
        if (src && 'setData' in src) {
          (src as any).setData({ type: 'FeatureCollection', features: points })
        }
      } catch {
        // Source may be removed during cleanup
      }
      frameId = requestAnimationFrame(tick)
    }

    frameId = requestAnimationFrame(tick)
    return () => {
      cancelAnimationFrame(frameId)
      removeLayer()
    }
  }, [linkAnimating, mapReady, isDark])

  // Colors
  const deviceColor = '#00ffcc' // vibrant cyan - matches globe view, overlays will override
  const metroColor = '#00ccaa' // muted cyan - matches globe view
  const validatorColor = '#a855f7' // purple - matches globe view
  const selectionColor = '#3b82f6' // blue - consistent with graph view

  // Build hover info for links
  const buildLinkInfo = useCallback((link: TopologyLink): HoveredLinkInfo => {
    const healthInfo = linkSlaStatus.get(link.pk)
    return {
      pk: link.pk,
      code: link.code,
      linkType: link.link_type,
      bandwidthBps: link.bandwidth_bps,
      latencyUs: link.latency_us,
      jitterUs: link.jitter_us ?? 0,
      latencyAtoZUs: link.latency_a_to_z_us,
      jitterAtoZUs: link.jitter_a_to_z_us ?? 0,
      latencyZtoAUs: link.latency_z_to_a_us,
      jitterZtoAUs: link.jitter_z_to_a_us ?? 0,
      lossPercent: link.loss_percent ?? 0,
      inBps: link.in_bps,
      outBps: link.out_bps,
      deviceAPk: link.side_a_pk,
      deviceACode: link.side_a_code || 'Unknown',
      interfaceAName: link.side_a_iface_name || '',
      interfaceAIP: link.side_a_ip || '',
      deviceZPk: link.side_z_pk,
      deviceZCode: link.side_z_code || 'Unknown',
      interfaceZName: link.side_z_iface_name || '',
      interfaceZIP: link.side_z_ip || '',
      contributorPk: link.contributor_pk,
      contributorCode: link.contributor_code,
      sampleCount: link.sample_count ?? 0,
      committedRttNs: link.committed_rtt_ns,
      isisDelayOverrideNs: link.isis_delay_override_ns,
      health: healthInfo ? {
        status: healthInfo.status,
        committedRttNs: healthInfo.committedRttNs,
        slaRatio: healthInfo.slaRatio,
        lossPct: healthInfo.lossPct,
      } : undefined,
    }
  }, [linkSlaStatus])

  // Handle map click to deselect or select links
  const handleMapClick = useCallback((e: MapLayerMouseEvent) => {
    // If a marker was clicked, don't process map click (marker handler takes precedence)
    if (markerClickedRef.current) {
      return
    }
    // Don't handle link clicks in path mode or addition mode
    if (pathModeEnabled || whatifAdditionMode) {
      return
    }
    // Check if a link was clicked
    if (e.features && e.features.length > 0) {
      const feature = e.features[0]
      if (feature.properties?.pk && feature.layer?.id?.includes('link')) {
        const pk = feature.properties.pk
        const link = linkMap.get(pk)
        if (link) {
          // Handle whatif-removal mode link click
          if (whatifRemovalMode) {
            setRemovalLink({
              sourcePK: link.side_a_pk,
              targetPK: link.side_z_pk,
              linkPK: link.pk,
            })
            return
          }
          handleMarkerClick({ type: 'link', data: buildLinkInfo(link) })
          return
        }
      }
    }
    // Close drawer when clicking empty area
    setSelectedItem(null)
  }, [setSelectedItem, linkMap, buildLinkInfo, handleMarkerClick, pathModeEnabled, whatifRemovalMode, whatifAdditionMode])

  // Map control handlers
  const handleZoomIn = useCallback(() => {
    mapRef.current?.zoomIn()
  }, [])

  const handleZoomOut = useCallback(() => {
    mapRef.current?.zoomOut()
  }, [])

  // Restore mode selections from URL params on initial load
  useEffect(() => {
    // Only run once when data and map are available
    if (modeParamsRestored) return
    if (deviceMap.size === 0 || linkMap.size === 0) return
    if (!mapReady) return

    const pathSourceParam = searchParams.get('path_source')
    const pathTargetParam = searchParams.get('path_target')
    const removalLinkParam = searchParams.get('removal_link')
    const additionSourceParam = searchParams.get('addition_source')
    const additionTargetParam = searchParams.get('addition_target')
    const impactDevicesParam = searchParams.get('impact_devices')
    const multicastParam = searchParams.get('multicast')

    // Restore path mode
    if (pathSourceParam || pathTargetParam) {
      // Check if all referenced devices are available
      const sourceFound = !pathSourceParam || deviceMap.has(pathSourceParam)
      const targetFound = !pathTargetParam || deviceMap.has(pathTargetParam)
      if (!sourceFound || !targetFound) {
        // Devices not found yet, wait for more data
        return
      }
      if (pathSourceParam) {
        setPathSource(pathSourceParam)
      }
      if (pathTargetParam) {
        setPathTarget(pathTargetParam)
      }
      if (mode !== 'path') {
        setMode('path')
        openPanel('mode')
      }
      // Fly to show selected device(s)
      const sourceDevice = pathSourceParam ? deviceMap.get(pathSourceParam) : undefined
      const targetDevice = pathTargetParam ? deviceMap.get(pathTargetParam) : undefined
      const sourceMetro = sourceDevice ? metroMap.get(sourceDevice.metro_pk) : undefined
      const targetMetro = targetDevice ? metroMap.get(targetDevice.metro_pk) : undefined
      if (sourceMetro && targetMetro && mapRef.current) {
        // Two devices - fly to midpoint
        const aLng = sourceMetro.longitude
        let zLng = targetMetro.longitude
        const lngDelta = zLng - aLng
        if (Math.abs(lngDelta) > 180) {
          zLng = lngDelta > 0 ? zLng - 360 : zLng + 360
        }
        let midLng = (aLng + zLng) / 2
        if (midLng > 180) midLng -= 360
        if (midLng < -180) midLng += 360
        const midLat = (sourceMetro.latitude + targetMetro.latitude) / 2
        mapRef.current.flyTo({ center: [midLng, midLat], zoom: 3, duration: 1000 })
      } else if (sourceMetro && mapRef.current) {
        mapRef.current.flyTo({ center: [sourceMetro.longitude, sourceMetro.latitude], zoom: 4, duration: 1000 })
      } else if (targetMetro && mapRef.current) {
        mapRef.current.flyTo({ center: [targetMetro.longitude, targetMetro.latitude], zoom: 4, duration: 1000 })
      }
      setModeParamsRestored(true)
      return
    }

    // Restore what-if removal mode
    if (removalLinkParam) {
      const link = linkMap.get(removalLinkParam)
      if (link) {
        setRemovalLink({ sourcePK: link.side_a_pk, targetPK: link.side_z_pk, linkPK: link.pk })
        if (mode !== 'whatif-removal') {
          setMode('whatif-removal')
          openPanel('mode')
        }
        // Fly to the link's midpoint
        const deviceA = deviceMap.get(link.side_a_pk)
        const deviceZ = deviceMap.get(link.side_z_pk)
        const metroA = deviceA ? metroMap.get(deviceA.metro_pk) : undefined
        const metroZ = deviceZ ? metroMap.get(deviceZ.metro_pk) : undefined
        if (metroA && metroZ && mapRef.current) {
          const aLng = metroA.longitude
          let zLng = metroZ.longitude
          const lngDelta = zLng - aLng
          if (Math.abs(lngDelta) > 180) {
            zLng = lngDelta > 0 ? zLng - 360 : zLng + 360
          }
          let midLng = (aLng + zLng) / 2
          if (midLng > 180) midLng -= 360
          if (midLng < -180) midLng += 360
          const midLat = (metroA.latitude + metroZ.latitude) / 2
          mapRef.current.flyTo({ center: [midLng, midLat], zoom: 3, duration: 1000 })
        }
        setModeParamsRestored(true)
        return
      }
      // Link not found yet, wait for more data
      return
    }

    // Restore what-if addition mode
    if (additionSourceParam || additionTargetParam) {
      // Check if all referenced devices are available
      const sourceFound = !additionSourceParam || deviceMap.has(additionSourceParam)
      const targetFound = !additionTargetParam || deviceMap.has(additionTargetParam)
      if (!sourceFound || !targetFound) {
        // Devices not found yet, wait for more data
        return
      }
      if (additionSourceParam) {
        setAdditionSource(additionSourceParam)
      }
      if (additionTargetParam) {
        setAdditionTarget(additionTargetParam)
      }
      if (mode !== 'whatif-addition') {
        setMode('whatif-addition')
        openPanel('mode')
      }
      // Fly to show selected device(s)
      const sourceDevice = additionSourceParam ? deviceMap.get(additionSourceParam) : undefined
      const targetDevice = additionTargetParam ? deviceMap.get(additionTargetParam) : undefined
      const sourceMetro = sourceDevice ? metroMap.get(sourceDevice.metro_pk) : undefined
      const targetMetro = targetDevice ? metroMap.get(targetDevice.metro_pk) : undefined
      if (sourceMetro && targetMetro && mapRef.current) {
        // Two devices - fly to midpoint
        const aLng = sourceMetro.longitude
        let zLng = targetMetro.longitude
        const lngDelta = zLng - aLng
        if (Math.abs(lngDelta) > 180) {
          zLng = lngDelta > 0 ? zLng - 360 : zLng + 360
        }
        let midLng = (aLng + zLng) / 2
        if (midLng > 180) midLng -= 360
        if (midLng < -180) midLng += 360
        const midLat = (sourceMetro.latitude + targetMetro.latitude) / 2
        mapRef.current.flyTo({ center: [midLng, midLat], zoom: 3, duration: 1000 })
      } else if (sourceMetro && mapRef.current) {
        mapRef.current.flyTo({ center: [sourceMetro.longitude, sourceMetro.latitude], zoom: 4, duration: 1000 })
      } else if (targetMetro && mapRef.current) {
        mapRef.current.flyTo({ center: [targetMetro.longitude, targetMetro.latitude], zoom: 4, duration: 1000 })
      }
      setModeParamsRestored(true)
      return
    }

    // Restore impact mode (comma-separated device PKs)
    if (impactDevicesParam) {
      const devicePKs = impactDevicesParam.split(',').filter(Boolean)
      // Check if all devices are available
      const allFound = devicePKs.every(pk => deviceMap.has(pk))
      if (!allFound) {
        // Some devices not found yet, wait for more data
        return
      }
      // Add each device to the impact list
      for (const pk of devicePKs) {
        if (!impactDevices.includes(pk)) {
          toggleImpactDevice(pk)
        }
      }
      if (mode !== 'impact') {
        setMode('impact')
        openPanel('mode')
      }
      // Fly to fit all impact devices
      if (devicePKs.length > 0 && mapRef.current) {
        // Collect all metro locations for the selected devices
        const locations: { lng: number; lat: number }[] = []
        for (const pk of devicePKs) {
          const device = deviceMap.get(pk)
          const metro = device ? metroMap.get(device.metro_pk) : undefined
          if (metro) {
            locations.push({ lng: metro.longitude, lat: metro.latitude })
          }
        }
        if (locations.length === 1) {
          // Single device - fly to it
          mapRef.current.flyTo({ center: [locations[0].lng, locations[0].lat], zoom: 4, duration: 1000 })
        } else if (locations.length > 1) {
          // Multiple devices - fit bounds to show all
          const lngs = locations.map(l => l.lng)
          const lats = locations.map(l => l.lat)
          const bounds: [[number, number], [number, number]] = [
            [Math.min(...lngs), Math.min(...lats)],
            [Math.max(...lngs), Math.max(...lats)],
          ]
          mapRef.current.fitBounds(bounds, { padding: 100, duration: 1000 })
        }
      }
      setModeParamsRestored(true)
      return
    }

    // Restore multicast group selection and disabled publishers/subscribers
    if (multicastParam) {
      const codes = multicastParam.split(',').filter(Boolean)
      if (codes.length > 0) {
        setSelectedMulticastGroup(codes[0] ?? null)
        if (!overlays.multicastTrees) toggleOverlay('multicastTrees')
        openPanel('overlay')
      }
      const pubOffParam = searchParams.get('mc_pub_off')
      const subOffParam = searchParams.get('mc_sub_off')
      if (pubOffParam) {
        initialDisabledPubsRef.current = new Set(pubOffParam.split(',').filter(Boolean))
      }
      if (subOffParam) {
        initialDisabledSubsRef.current = new Set(subOffParam.split(',').filter(Boolean))
      }
    }

    // No mode params to restore
    setModeParamsRestored(true)
  }, [modeParamsRestored, searchParams, deviceMap, linkMap, metroMap, mapReady, mode, setMode, openPanel, impactDevices, toggleImpactDevice, overlays.multicastTrees, toggleOverlay])

  // Restore selected item from URL params when they change
  // Track last processed params to avoid re-processing the same selection
  const lastProcessedParamsRef = useRef<string | null>(null)
  useEffect(() => {
    const type = searchParams.get('type')
    const id = searchParams.get('id')

    // No params to restore
    if (!type || !id) {
      return
    }

    // Wait for map to be ready before restoring selection (need map for flyTo)
    if (!mapReady) {
      return
    }

    // Skip if we already processed these exact params
    const paramsKey = `${type}:${id}`
    if (lastProcessedParamsRef.current === paramsKey) {
      return
    }

    // Helper to fly to location (gentle zoom - just enough to see the item)
    const getPanelOffsetX = () => {
      if (!panel.isOpen || panel.content !== 'details') return 0
      const sidebarCollapsed = typeof window === 'undefined'
        ? true
        : window.localStorage.getItem('sidebar-collapsed') !== 'false'
      const sidebarWidth = sidebarCollapsed ? 48 : 256
      const leftPad = sidebarWidth + panel.width + 16
      const rightPad = 176 + 16 // Controls panel max width + margin
      return Math.round((leftPad - rightPad) / 2)
    }

    const flyToLocation = (lng: number, lat: number, zoom = 3, usePanelOffset = false) => {
      if (mapRef.current) {
        mapRef.current.flyTo({
          center: [lng, lat],
          zoom,
          duration: 1000,
          offset: usePanelOffset ? [getPanelOffsetX(), 0] : [0, 0],
        })
      }
    }

    const linkFitsInView = (aLng: number, aLat: number, zLng: number, zLat: number) => {
      if (!mapRef.current) return false
      const bounds = mapRef.current.getMap().getBounds()
      return bounds.contains([aLng, aLat]) && bounds.contains([zLng, zLat])
    }

    let itemFound = false

    if (type === 'validator') {
      const validator = validatorMap.get(id)
      if (validator) {
        const device = deviceMap.get(validator.device_pk)
        const metro = device ? metroMap.get(device.metro_pk) : undefined
        setSelectedItemState({
          type: 'validator',
          data: {
            votePubkey: validator.vote_pubkey,
            nodePubkey: validator.node_pubkey,
            tunnelId: validator.tunnel_id,
            city: validator.city || 'Unknown',
            country: validator.country || 'Unknown',
            stakeSol: (validator.stake_sol ?? 0) >= 1e6 ? `${(validator.stake_sol / 1e6).toFixed(2)}M` : (validator.stake_sol ?? 0) >= 1e3 ? `${(validator.stake_sol / 1e3).toFixed(0)}k` : `${(validator.stake_sol ?? 0).toFixed(0)}`,
            stakeShare: (validator.stake_share ?? 0) > 0 ? `${validator.stake_share.toFixed(2)}%` : '0%',
            commission: validator.commission ?? 0,
            version: validator.version || '',
            gossipIp: validator.gossip_ip || '',
            gossipPort: validator.gossip_port ?? 0,
            tpuQuicIp: validator.tpu_quic_ip || '',
            tpuQuicPort: validator.tpu_quic_port ?? 0,
            deviceCode: device?.code || 'Unknown',
            devicePk: validator.device_pk,
            metroPk: device?.metro_pk || '',
            metroName: metro?.name || 'Unknown',
            inRate: formatTrafficRate(validator.in_bps),
            outRate: formatTrafficRate(validator.out_bps),
          },
        })
        if (!showValidators) toggleOverlay('validators') // Show validators layer when loading a validator from URL
        // Fly to validator's metro location (skip in analysis modes)
        if (metro && !pathModeEnabled && !impactMode) {
          flyToLocation(metro.longitude, metro.latitude, 4)
        }
        itemFound = true
      }
    } else if (type === 'device') {
      const device = deviceMap.get(id)
      if (device) {
        const metro = metroMap.get(device.metro_pk)
        setSelectedItemState({
          type: 'device',
          data: {
            pk: device.pk,
            code: device.code,
            deviceType: device.device_type,
            status: device.status,
            metroPk: device.metro_pk,
            metroName: metro?.name || 'Unknown',
            contributorPk: device.contributor_pk,
            contributorCode: device.contributor_code,
            userCount: device.user_count ?? 0,
            validatorCount: device.validator_count ?? 0,
            stakeSol: device.stake_sol ?? 0,
            stakeShare: device.stake_share ?? 0,
            interfaces: device.interfaces || [],
          },
        })
        // Fly to device's metro location (skip in analysis modes)
        if (metro && !pathModeEnabled && !impactMode) {
          flyToLocation(metro.longitude, metro.latitude, 4)
        }
        itemFound = true

        // Handle mode-specific actions for device selection via search
        if (impactMode) {
          if (!impactDevices.includes(id)) {
            toggleImpactDevice(id)
          }
        } else if (pathModeEnabled) {
          if (!pathSource) {
            setPathSource(id)
          } else if (!pathTarget && id !== pathSource) {
            setPathTarget(id)
          }
        } else if (whatifAdditionMode) {
          if (!additionSource) {
            setAdditionSource(id)
          } else if (!additionTarget && id !== additionSource) {
            setAdditionTarget(id)
          }
        }
      }
    } else if (type === 'link') {
      const link = linkMap.get(id)
      if (link) {
        const deviceA = deviceMap.get(link.side_a_pk)
        const deviceZ = deviceMap.get(link.side_z_pk)
        setSelectedItemState({
          type: 'link',
          data: buildLinkInfo(link),
        })
        // Fly to the midpoint of the link (skip in analysis modes)
        if (!pathModeEnabled && !impactMode && !whatifRemovalMode) {
          const metroA = deviceA ? metroMap.get(deviceA.metro_pk) : undefined
          const metroZ = deviceZ ? metroMap.get(deviceZ.metro_pk) : undefined
          if (metroA && metroZ) {
            if (linkFitsInView(metroA.longitude, metroA.latitude, metroZ.longitude, metroZ.latitude)) {
              itemFound = true
            } else {
            const aLng = metroA.longitude
            let zLng = metroZ.longitude
            const lngDelta = zLng - aLng
            if (Math.abs(lngDelta) > 180) {
              zLng = lngDelta > 0 ? zLng - 360 : zLng + 360
            }
            let midLng = (aLng + zLng) / 2
            if (midLng > 180) midLng -= 360
            if (midLng < -180) midLng += 360
            const midLat = (metroA.latitude + metroZ.latitude) / 2
            flyToLocation(midLng, midLat, 3, true)
            }
          } else if (metroA) {
            flyToLocation(metroA.longitude, metroA.latitude, 3, true)
          }
        }
        itemFound = true

        // Handle mode-specific actions for link selection via search
        if (whatifRemovalMode) {
          setRemovalLink({ sourcePK: link.side_a_pk, targetPK: link.side_z_pk, linkPK: link.pk })
        }
      }
    } else if (type === 'metro') {
      const metro = metroMap.get(id)
      if (metro) {
        const metroDeviceCount = devicesByMetro.get(metro.pk)?.length || 0
        setSelectedItemState({
          type: 'metro',
          data: {
            pk: metro.pk,
            code: metro.code,
            name: metro.name,
            deviceCount: metroDeviceCount,
          },
        })
        // Fly to metro location
        flyToLocation(metro.longitude, metro.latitude, 4)
        itemFound = true
      }
    }

    // Open the details panel if item was found (in explore mode)
    if (itemFound && mode === 'explore') {
      openPanel('details')
    }

    // Mark as processed once we've successfully found and selected the item
    if (itemFound) {
      lastProcessedParamsRef.current = paramsKey
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams, mapReady, validatorMap, deviceMap, linkMap, metroMap, devicesByMetro, showValidators, toggleOverlay, mode, openPanel, impactMode, pathModeEnabled, whatifAdditionMode, whatifRemovalMode, pathSource, pathTarget, additionSource, additionTarget])

  // Handle link layer hover
  const handleLinkMouseEnter = useCallback((e: MapLayerMouseEvent) => {
    if (e.features && e.features[0]) {
      const pk = e.features[0].properties?.pk

      // Skip hover on dimmed links when multicast overlay is active
      if (multicastTreesMode && dimOtherLinks && pk && !multicastTreeLinkPKs.has(pk)) {
        return
      }

      const props = e.features[0].properties

      // Handle inter-metro links
      if (pk?.startsWith('inter-metro-') && props?.isInterMetro) {
        setHoveredLink({
          pk,
          code: props.code || '',
          linkType: 'Inter-Metro',
          bandwidthBps: 0,
          latencyUs: props.avgLatencyUs || 0,
          jitterUs: 0,
          latencyAtoZUs: 0,
          jitterAtoZUs: 0,
          latencyZtoAUs: 0,
          jitterZtoAUs: 0,
          lossPercent: 0,
          inBps: 0,
          outBps: 0,
          deviceAPk: '',
          deviceACode: '',
          interfaceAName: '',
          interfaceAIP: '',
          deviceZPk: '',
          deviceZCode: '',
          interfaceZName: '',
          interfaceZIP: '',
          contributorPk: '',
          contributorCode: '',
          sampleCount: 0,
          committedRttNs: 0,
          isisDelayOverrideNs: 0,
          isInterMetro: true,
          linkCount: props.linkCount || 0,
          avgLatencyUs: props.avgLatencyUs || 0,
        })
        return
      }

      const link = linkMap.get(pk)
      if (link) {
        setHoveredLink(buildLinkInfo(link))
      }
    }
  }, [linkMap, buildLinkInfo, multicastTreesMode, dimOtherLinks, multicastTreeLinkPKs])

  const handleLinkMouseLeave = useCallback(() => {
    setHoveredLink(null)
  }, [])

  // Track mouse position for cursor-following popover
  const handleMouseMove = useCallback((e: maplibregl.MapMouseEvent) => {
    setMousePos({ x: e.point.x, y: e.point.y })
  }, [])

  return (
    <>
      <MapGL
        ref={mapRef}
        initialViewState={{
          longitude: 0,
          latitude: 30,
          zoom: 2,
        }}
        minZoom={2}
        maxZoom={18}
        mapStyle={mapStyle}
        style={{ width: '100%', height: '100%' }}
        attributionControl={false}
        onClick={handleMapClick}
        interactiveLayerIds={['link-lines', 'link-hit-area']}
        onMouseEnter={handleLinkMouseEnter}
        onMouseLeave={handleLinkMouseLeave}
        onMouseMove={handleMouseMove}
        onLoad={() => setMapReady(true)}
        cursor={hoveredLink ? 'pointer' : undefined}
      >
        <TopologyControlBar
          onZoomIn={handleZoomIn}
          onZoomOut={handleZoomOut}
          onReset={fitBounds}
          linkAnimating={linkAnimating}
          onToggleLinkAnimation={() => setLinkAnimating(prev => !prev)}
        />

        {/* Link lines source and layers */}
        <Source id="links" type="geojson" data={linkGeoJson}>
          {/* Invisible hit area for easier clicking */}
          <Layer
            id="link-hit-area"
            type="line"
            paint={{
              'line-color': 'transparent',
              'line-width': 12,
            }}
            layout={{
              'line-cap': 'round',
              'line-join': 'round',
            }}
          />
          {/* Selection halo - renders before main lines */}
          <Layer
            id="link-selection-halo"
            type="line"
            filter={['==', ['get', 'isSelected'], 1]}
            paint={{
              'line-color': '#3b82f6',
              'line-width': ['+', ['get', 'weight'], 8],
              'line-opacity': 0.3,
            }}
            layout={{
              'line-cap': 'round',
              'line-join': 'round',
            }}
          />
          {/* Render solid links (useDash = false) */}
          <Layer
            id="link-lines"
            type="line"
            filter={['!=', ['get', 'useDash'], true]}
            paint={{
              'line-color': ['get', 'color'],
              'line-width': ['get', 'weight'],
              'line-opacity': ['get', 'opacity'],
            }}
            layout={{
              'line-cap': 'round',
              'line-join': 'round',
            }}
          />
          {/* Render dashed links (useDash = true) - for shared multicast paths */}
          <Layer
            id="link-lines-dashed"
            type="line"
            filter={['==', ['get', 'useDash'], true]}
            paint={{
              'line-color': ['get', 'color'],
              'line-width': ['get', 'weight'],
              'line-opacity': ['get', 'opacity'],
              'line-dasharray': [4, 3],
            }}
            layout={{
              'line-cap': 'round',
              'line-join': 'round',
            }}
          />
        </Source>

        {/* Per-publisher multicast tree lines (each publisher gets its own offset curve) */}
        {multicastTreesMode && multicastTreeGeoJson.features.length > 0 && (
          <Source id="multicast-tree-lines" type="geojson" data={multicastTreeGeoJson}>
            <Layer
              id="multicast-tree-lines-layer"
              type="line"
              paint={{
                'line-color': ['get', 'color'],
                'line-width': ['get', 'weight'],
                'line-opacity': ['get', 'opacity'],
              }}
              layout={{
                'line-cap': 'round',
                'line-join': 'round',
              }}
            />
          </Source>
        )}

        {/* Animated multicast flow dots are managed imperatively via useEffect
            (source: multicast-flow-dots) to avoid React reconciliation issues. */}

        {/* Validator links (when toggled, hidden in path mode, or auto-shown for multicast tree) */}
        {visibleValidators.length > 0 && (
          <Source id="validator-links" type="geojson" data={validatorLinksGeoJson}>
            <Layer
              id="validator-link-lines"
              type="line"
              paint={{
                'line-color': ['get', 'color'],
                'line-width': ['get', 'weight'],
                'line-opacity': ['get', 'opacity'],
                'line-dasharray': [4, 4],
              }}
            />
          </Source>
        )}

        {/* Metro markers - only show when metros overlay is active */}
        {metroClusteringMode && metros.map(metro => {
          const isThisHovered = hoveredMetro?.code === metro.code
          const isThisSelected = selectedItem?.type === 'metro' && selectedItem.data.pk === metro.pk
          const metroDeviceCount = devicesByMetro.get(metro.pk)?.length || 0
          const metroInfo: HoveredMetroInfo = {
            pk: metro.pk,
            code: metro.code,
            name: metro.name,
            deviceCount: metroDeviceCount,
          }

          return (
            <Marker
              key={`metro-${metro.pk}`}
              longitude={metro.longitude}
              latitude={metro.latitude}
              anchor="center"
            >
              <div
                className="rounded-full cursor-pointer transition-all"
                style={{
                  width: isThisSelected ? 28 : isThisHovered ? 26 : 24,
                  height: isThisSelected ? 28 : isThisHovered ? 26 : 24,
                  backgroundColor: metroColor,
                  opacity: isThisHovered || isThisSelected ? 0.5 : 0.3,
                  border: `${isThisSelected ? 3 : isThisHovered ? 2 : 1}px solid ${isThisSelected ? selectionColor : isThisHovered ? hoverHighlight : metroColor}`,
                  boxShadow: isThisSelected
                    ? `0 0 0 4px ${selectionColor}40, 0 0 12px ${selectionColor}60`
                    : isThisHovered
                      ? `0 0 0 2px ${hoverHighlight}40`
                      : undefined,
                }}
                onMouseEnter={() => {
                  setHoveredMetro(metroInfo)
                  setHoveredLink(null)
                }}
                onMouseLeave={() => setHoveredMetro(null)}
                onClick={() => handleMarkerClick({ type: 'metro', data: metroInfo })}
              />
            </Marker>
          )
        })}

        {/* Device markers - show disabled state for non-ISIS devices in path mode */}
        {devices.map(device => {
          const pos = devicePositions.get(device.pk)
          if (!pos) return null

          // Hide device if its metro is collapsed
          if (metroClusteringMode && collapsedMetros.has(device.metro_pk)) {
            return null
          }

          const metro = metroMap.get(device.metro_pk)
          const isThisHovered = hoveredDevice?.code === device.code
          const isThisSelected = selectedItem?.type === 'device' && selectedItem.data.pk === device.pk
          const isPathSource = pathSource === device.pk
          const isPathTarget = pathTarget === device.pk
          const devicePathIndices = devicePathMap.get(device.pk)
          const isInSelectedPath = devicePathIndices?.includes(selectedPathIndex)
          const isInAnyPath = devicePathIndices && devicePathIndices.length > 0
          // Metro path mode
          const metroDevicePathIndices = metroDevicePathMap.get(device.pk)
          const isInSelectedMetroPath = metroPathSelectedPairs.length > 0 && metroDevicePathIndices?.some(idx => metroPathSelectedPairs.includes(idx))
          const isInAnyMetroPath = metroDevicePathIndices && metroDevicePathIndices.length > 0
          // Multicast tree mode
          const isInAnyMulticastTree = multicastTreeDevicePKs.has(device.pk)
          const isMulticastPublisher = multicastPublisherDevices.has(device.pk)
          const isMulticastSubscriber = multicastSubscriberDevices.has(device.pk)
          // Check if device has ISIS data (can participate in path finding)
          const isISISEnabled = isisDevicePKs.size === 0 || isisDevicePKs.has(device.pk)
          const isDisabledInPathMode = pathModeEnabled && !isISISEnabled
          // What-If mode states
          const isAdditionSource = additionSource === device.pk
          const isAdditionTarget = additionTarget === device.pk
          const isDisconnected = disconnectedDevicePKs.has(device.pk)
          // Impact mode state
          const isImpactDevice = impactMode && impactDevices.includes(device.pk)
          const deviceInfo: HoveredDeviceInfo = {
            pk: device.pk,
            code: device.code,
            deviceType: device.device_type,
            status: device.status,
            metroPk: device.metro_pk,
            metroName: metro?.name || 'Unknown',
            contributorPk: device.contributor_pk,
            contributorCode: device.contributor_code,
            userCount: device.user_count ?? 0,
            validatorCount: device.validator_count ?? 0,
            stakeSol: device.stake_sol ?? 0,
            stakeShare: device.stake_share ?? 0,
            interfaces: device.interfaces || [],
          }

          // Determine marker styling based on path state
          let markerColor = deviceColor
          let markerSize = 12
          let borderWidth = 1
          let opacity = 0.9
          let borderColor = hoverHighlight

          // Device type mode - color based on device type (default overlay)
          if (deviceTypeMode && !stakeOverlayMode && !metroClusteringMode && !contributorDevicesMode) {
            const deviceType = device.device_type?.toLowerCase() || 'default'
            markerColor = DEVICE_TYPE_COLORS[deviceType] || DEVICE_TYPE_COLORS.default
            borderColor = markerColor
            borderWidth = 2
            opacity = 1
          }

          // Stake overlay mode - size and color based on stake
          if (stakeOverlayMode) {
            const stakeSol = device.stake_sol ?? 0
            const stakeShare = device.stake_share ?? 0
            markerSize = calculateDeviceStakeSize(stakeSol)
            markerColor = getStakeColor(stakeShare)
            borderColor = markerColor
            borderWidth = stakeSol > 0 ? 2 : 1
            opacity = stakeSol > 0 ? 1 : 0.4
          }

          // Metro clustering mode - color based on metro
          if (metroClusteringMode && !stakeOverlayMode && !contributorDevicesMode) {
            const metroIndex = metroIndexMap.get(device.metro_pk) ?? 0
            markerColor = getMetroColor(device.metro_pk, metroIndex)
            borderColor = markerColor
            borderWidth = 2
            opacity = 1
          }

          // Contributor devices mode - color based on contributor
          if (contributorDevicesMode && !stakeOverlayMode && !metroClusteringMode) {
            const contributorIndex = contributorIndexMap.get(device.contributor_pk) ?? 0
            markerColor = getContributorColor(device.contributor_pk, contributorIndex)
            borderColor = markerColor
            borderWidth = 2
            opacity = device.contributor_pk ? 1 : 0.4
          }

          if (isDisabledInPathMode) {
            // Grey out non-ISIS devices in path mode
            markerColor = isDark ? '#4b5563' : '#9ca3af' // gray
            borderColor = markerColor
            markerSize = 10
            opacity = 0.4
          } else if (whatifRemovalMode && isDisconnected) {
            // Disconnected device in whatif-removal mode
            markerColor = '#ef4444' // red
            borderColor = '#ef4444'
            markerSize = 16
            borderWidth = 3
            opacity = 1
          } else if (isAdditionSource) {
            markerColor = '#22c55e' // green
            borderColor = '#22c55e'
            markerSize = 18
            borderWidth = 3
            opacity = 1
          } else if (isAdditionTarget) {
            markerColor = '#ef4444' // red
            borderColor = '#ef4444'
            markerSize = 18
            borderWidth = 3
            opacity = 1
          } else if (isPathSource) {
            markerColor = '#22c55e' // green
            borderColor = '#22c55e'
            markerSize = 18
            borderWidth = 3
            opacity = 1
          } else if (isPathTarget) {
            markerColor = '#ef4444' // red
            borderColor = '#ef4444'
            markerSize = 18
            borderWidth = 3
            opacity = 1
          } else if (isInSelectedPath && devicePathIndices) {
            // Use selected path's color
            markerColor = PATH_COLORS[selectedPathIndex % PATH_COLORS.length]
            borderColor = markerColor
            markerSize = 16
            borderWidth = 2
            opacity = 1
          } else if (isInAnyPath && devicePathIndices) {
            // In another path but not selected
            const firstPathIndex = devicePathIndices[0]
            markerColor = PATH_COLORS[firstPathIndex % PATH_COLORS.length]
            borderColor = markerColor
            markerSize = 14
            borderWidth = 1
            opacity = 0.5
          } else if (metroPathModeEnabled && isInSelectedMetroPath && metroDevicePathIndices) {
            // Metro path: in selected pair's path
            markerColor = PATH_COLORS[0]
            borderColor = markerColor
            markerSize = 16
            borderWidth = 2
            opacity = 1
          } else if (metroPathModeEnabled && isInAnyMetroPath && metroDevicePathIndices) {
            // Metro path: in any path but not selected - show dimmed
            const firstPairIndex = metroDevicePathIndices[0]
            markerColor = PATH_COLORS[firstPairIndex % PATH_COLORS.length]
            borderColor = markerColor
            markerSize = 14
            borderWidth = 1
            opacity = 0.4
          } else if (multicastTreesMode && isMulticastPublisher) {
            // Multicast publisher: color by publisher's assigned color
            const publisherColorIndex = multicastPublisherColorMap.get(device.pk) ?? 0
            const mcPub = MULTICAST_PUBLISHER_COLORS[publisherColorIndex % MULTICAST_PUBLISHER_COLORS.length]
            markerColor = isDark ? mcPub.dark : mcPub.light
            borderColor = markerColor
            markerSize = 20
            borderWidth = 4
            opacity = 1
          } else if (multicastTreesMode && isMulticastSubscriber) {
            // Multicast subscriber: keep red to distinguish from publishers
            markerColor = '#ef4444' // red
            borderColor = '#ef4444'
            markerSize = 18
            borderWidth = 3
            opacity = 1
          } else if (multicastTreesMode && isInAnyMulticastTree) {
            // Multicast transit: in tree path, color by first publisher found for this device
            const firstPub = multicastPublisherPaths.find(pp => pp.segments.some(s => s.fromPK === device.pk || s.toPK === device.pk))
            const firstPubPK = firstPub?.publisherPK
            const publisherColorIndex = firstPubPK ? (multicastPublisherColorMap.get(firstPubPK) ?? 0) : 0
            const mcTransit = MULTICAST_PUBLISHER_COLORS[publisherColorIndex % MULTICAST_PUBLISHER_COLORS.length]
            markerColor = isDark ? mcTransit.dark : mcTransit.light
            borderColor = markerColor
            markerSize = 14
            borderWidth = 2
            opacity = 0.9
          } else if (isImpactDevice) {
            // Impact device being analyzed for failure
            markerColor = '#ef4444' // red
            borderColor = '#ef4444'
            markerSize = 18
            borderWidth = 3
            opacity = 1
          } else if (isThisSelected) {
            // Selected: blue border with halo
            borderColor = selectionColor
            markerSize = 14
            borderWidth = 3
            opacity = 1
          } else if (isThisHovered) {
            // Hovered: amber highlight
            borderColor = hoverHighlight
            markerSize = 14
            borderWidth = 2
            opacity = 1
          }

          // Selection/impact halo effect
          const boxShadow = isImpactDevice
            ? `0 0 0 4px #ef444440, 0 0 12px #ef444460`
            : isThisSelected
              ? `0 0 0 4px ${selectionColor}40, 0 0 12px ${selectionColor}60`
              : isThisHovered
                ? `0 0 0 2px ${hoverHighlight}40`
                : undefined

          // Multicast P/S labels
          const pubCount = multicastPublisherCount.get(device.pk) || 0
          const subCount = multicastSubscriberCount.get(device.pk) || 0
          const showMulticastLabel = multicastTreesMode && (isMulticastPublisher || isMulticastSubscriber)

          // Build title for tooltip
          let tooltipTitle = isDisabledInPathMode ? 'No ISIS data - cannot use for path finding' : undefined
          if (multicastTreesMode && (pubCount > 0 || subCount > 0)) {
            const parts: string[] = []
            if (pubCount > 0) parts.push(`${pubCount} publisher${pubCount > 1 ? 's' : ''}`)
            if (subCount > 0) parts.push(`${subCount} subscriber${subCount > 1 ? 's' : ''}`)
            tooltipTitle = parts.join(', ')
          }

          return (
            <Marker
              key={`device-${device.pk}`}
              longitude={pos[0]}
              latitude={pos[1]}
              anchor="center"
            >
              <div
                className={`relative transition-all ${isDisabledInPathMode ? 'cursor-not-allowed' : 'cursor-pointer'}`}
                onMouseEnter={() => {
                  setHoveredDevice(deviceInfo)
                  setHoveredLink(null) // Clear link hover so device takes priority
                }}
                onMouseLeave={() => setHoveredDevice(null)}
                title={tooltipTitle}
                onClick={() => {
                  markerClickedRef.current = true
                  setTimeout(() => { markerClickedRef.current = false }, 0)
                  if (pathModeEnabled) {
                    // Only allow clicking ISIS-enabled devices in path mode
                    if (!isDisabledInPathMode) {
                      handlePathDeviceClick(device.pk)
                    }
                  } else if (whatifAdditionMode) {
                    // Handle whatif-addition mode device clicks
                    if (!additionSource) {
                      setAdditionSource(device.pk)
                    } else if (!additionTarget && device.pk !== additionSource) {
                      setAdditionTarget(device.pk)
                    } else {
                      // Reset and start new addition
                      setAdditionSource(device.pk)
                      setAdditionTarget(null)
                      setAdditionResult(null)
                    }
                  } else if (impactMode) {
                    // Handle impact mode device clicks - toggle device in/out of selection
                    toggleImpactDevice(device.pk)
                  } else {
                    handleMarkerClick({ type: 'device', data: deviceInfo })
                  }
                }}
              >
                {/* Device circle */}
                <div
                  className="rounded-full"
                  style={{
                    width: markerSize,
                    height: markerSize,
                    backgroundColor: markerColor,
                    border: `${borderWidth}px solid ${borderColor}`,
                    opacity,
                    boxShadow,
                  }}
                />
                {/* Multicast P/S label */}
                {showMulticastLabel && (
                  <div
                    className="absolute flex items-center justify-center font-bold text-white pointer-events-none"
                    style={{
                      top: -6,
                      right: -6,
                      minWidth: 14,
                      height: 14,
                      fontSize: 9,
                      borderRadius: 7,
                      backgroundColor: isMulticastPublisher ? markerColor : '#ef4444',
                      border: `1px solid ${isDark ? '#1f2937' : '#ffffff'}`,
                      padding: '0 2px',
                    }}
                  >
                    {isMulticastPublisher && isMulticastSubscriber ? (
                      <>P<span className="text-[7px]">+</span>S</>
                    ) : isMulticastPublisher ? (
                      pubCount > 1 ? `P${pubCount}` : 'P'
                    ) : (
                      subCount > 1 ? `S${subCount}` : 'S'
                    )}
                  </div>
                )}
              </div>
            </Marker>
          )
        })}

        {/* Super markers for collapsed metros */}
        {metroClusteringMode && metros.map(metro => {
          if (!collapsedMetros.has(metro.pk)) return null

          const metroIndex = metroIndexMap.get(metro.pk) ?? 0
          const metroColor = getMetroColor(metro.pk, metroIndex)
          const metroDevices = devicesByMetro.get(metro.pk) || []
          const deviceCount = metroDevices.length

          return (
            <Marker
              key={`super-metro-${metro.pk}`}
              longitude={metro.longitude}
              latitude={metro.latitude}
              anchor="center"
            >
              <div
                className="rounded-full cursor-pointer transition-all flex items-center justify-center text-white font-bold text-xs shadow-lg"
                style={{
                  width: 32,
                  height: 32,
                  backgroundColor: metroColor,
                  border: `3px solid ${metroColor}`,
                  opacity: 1,
                }}
                onClick={() => toggleMetroCollapse(metro.pk)}
                title={`${metro.name} (${deviceCount} devices) - Click to expand`}
              >
                {deviceCount}
              </div>
            </Marker>
          )
        })}

        {/* Validator markers (when toggled, hidden in path mode, or auto-shown for multicast tree) */}
        {visibleValidators.length > 0 && visibleValidators.map(validator => {
          const devicePos = devicePositions.get(validator.device_pk)
          if (!devicePos) return null

          const device = deviceMap.get(validator.device_pk)
          const metro = device ? metroMap.get(device.metro_pk) : undefined
          const isThisHovered = hoveredValidator?.votePubkey === validator.vote_pubkey
          const isThisSelected = selectedItem?.type === 'validator' && selectedItem.data.votePubkey === validator.vote_pubkey
          const baseRadius = calculateValidatorRadius(validator.stake_sol)
          const validatorInfo: HoveredValidatorInfo = {
            votePubkey: validator.vote_pubkey,
            nodePubkey: validator.node_pubkey,
            tunnelId: validator.tunnel_id,
            city: validator.city || 'Unknown',
            country: validator.country || 'Unknown',
            stakeSol: (validator.stake_sol ?? 0) >= 1e6 ? `${(validator.stake_sol / 1e6).toFixed(2)}M` : (validator.stake_sol ?? 0) >= 1e3 ? `${(validator.stake_sol / 1e3).toFixed(0)}k` : `${(validator.stake_sol ?? 0).toFixed(0)}`,
            stakeShare: (validator.stake_share ?? 0) > 0 ? `${validator.stake_share.toFixed(2)}%` : '0%',
            commission: validator.commission ?? 0,
            version: validator.version || '',
            gossipIp: validator.gossip_ip || '',
            gossipPort: validator.gossip_port ?? 0,
            tpuQuicIp: validator.tpu_quic_ip || '',
            tpuQuicPort: validator.tpu_quic_port ?? 0,
            deviceCode: device?.code || 'Unknown',
            devicePk: validator.device_pk,
            metroPk: device?.metro_pk || '',
            metroName: metro?.name || 'Unknown',
            inRate: formatTrafficRate(validator.in_bps),
            outRate: formatTrafficRate(validator.out_bps),
          }

          const size = (isThisSelected ? baseRadius + 3 : isThisHovered ? baseRadius + 2 : baseRadius) * 2
          const roleColor = multicastDeviceRoleColorMap.get(validator.device_pk)

          return (
            <Marker
              key={`validator-${validator.vote_pubkey}`}
              longitude={validator.longitude}
              latitude={validator.latitude}
              anchor="center"
            >
              <div
                className="rounded-full cursor-pointer transition-all"
                style={{
                  width: size,
                  height: size,
                  backgroundColor: validatorColor,
                  border: `${isThisSelected ? 3 : isThisHovered ? 2 : roleColor ? 2 : 1}px solid ${isThisSelected ? selectionColor : isThisHovered ? hoverHighlight : roleColor || hoverHighlight}`,
                  opacity: isThisHovered || isThisSelected ? 1 : 0.9,
                  boxShadow: isThisSelected
                    ? `0 0 0 4px ${selectionColor}40, 0 0 12px ${selectionColor}60`
                    : isThisHovered
                      ? `0 0 0 2px ${hoverHighlight}40`
                      : roleColor
                        ? `0 0 0 3px ${roleColor}50`
                        : undefined,
                }}
                onMouseEnter={() => {
                  setHoveredValidator(validatorInfo)
                  setHoveredLink(null)
                }}
                onMouseLeave={() => setHoveredValidator(null)}
                onClick={() => handleMarkerClick({ type: 'validator', data: validatorInfo })}
              />
            </Marker>
          )
        })}
      </MapGL>

      {/* Hover tooltip - cursor-following */}
      {(hoveredLink || hoveredDevice || hoveredMetro || hoveredValidator) && (
        <div
          className="absolute z-[1000] bg-[var(--card)]/95 backdrop-blur border border-[var(--border)] rounded-md shadow-lg px-3 py-2 pointer-events-none"
          style={{
            left: mousePos.x + 16,
            top: mousePos.y + 16,
          }}
        >
          {hoveredLink && (
            <div className="space-y-1">
              <div className="text-sm font-medium">{hoveredLink.code}</div>
              <div className="text-xs text-muted-foreground space-y-0.5">
                {!hoveredLink.isInterMetro && hoveredLink.deviceACode && (
                  <div>A-Side: <span className="text-foreground">{hoveredLink.deviceACode}</span>{hoveredLink.interfaceAName && <span className="text-foreground font-mono"> ({hoveredLink.interfaceAName}{hoveredLink.interfaceAIP && ` ${hoveredLink.interfaceAIP}`})</span>}</div>
                )}
                {!hoveredLink.isInterMetro && hoveredLink.deviceZCode && (
                  <div>Z-Side: <span className="text-foreground">{hoveredLink.deviceZCode}</span>{hoveredLink.interfaceZName && <span className="text-foreground font-mono"> ({hoveredLink.interfaceZName}{hoveredLink.interfaceZIP && ` ${hoveredLink.interfaceZIP}`})</span>}</div>
                )}
                <div>Type: <span className="text-foreground">{hoveredLink.isInterMetro ? 'Inter-Metro' : hoveredLink.linkType}</span></div>
                {hoveredLink.contributorCode && (
                  <div>Contributor: <span className="text-foreground">{hoveredLink.contributorCode}</span></div>
                )}
                {hoveredLink.isInterMetro ? (
                  <>
                    <div>Links: <span className="text-foreground">{hoveredLink.linkCount}</span></div>
                    <div>Avg Latency: <span className="text-foreground">{hoveredLink.avgLatencyUs ? `${(hoveredLink.avgLatencyUs / 1000).toFixed(2)} ms` : 'N/A'}</span></div>
                  </>
                ) : (
                  <>
                    <div>Latency: <span className="text-foreground">{hoveredLink.latencyUs > 0 ? `${(hoveredLink.latencyUs / 1000).toFixed(2)} ms` : 'N/A'}</span></div>
                    <div>Bandwidth: <span className="text-foreground">{formatBandwidth(hoveredLink.bandwidthBps)}</span></div>
                  </>
                )}
              </div>
            </div>
          )}
          {hoveredDevice && !hoveredLink && (
            <div className="space-y-1">
              <div className="text-sm font-medium">{hoveredDevice.code}</div>
              <div className="text-xs text-muted-foreground space-y-0.5">
                <div>Type: <span className="text-foreground capitalize">{hoveredDevice.deviceType}</span></div>
                {hoveredDevice.contributorCode && (
                  <div>Contributor: <span className="text-foreground">{hoveredDevice.contributorCode}</span></div>
                )}
              </div>
            </div>
          )}
          {hoveredMetro && !hoveredLink && !hoveredDevice && !hoveredValidator && (
            <div>
              <div className="text-xs text-muted-foreground">Metro</div>
              <div className="text-sm font-medium">{hoveredMetro.name}</div>
            </div>
          )}
          {hoveredValidator && !hoveredLink && !hoveredDevice && (
            <div>
              <div className="text-xs text-muted-foreground">Validator</div>
              <div className="text-sm font-medium font-mono">{hoveredValidator.votePubkey.slice(0, 12)}...</div>
            </div>
          )}
        </div>
      )}

      {/* Detail panel (right side) - shown when entity selected in explore mode */}
      {selectedItem && panel.isOpen && panel.content === 'details' && (
        <TopologyPanel
          title={
            selectedItem.type === 'device' ? (
              <TopologyEntityLink to={`/dz/devices/${selectedItem.data.pk}`}>{selectedItem.data.code}</TopologyEntityLink>
            ) :
            selectedItem.type === 'link' ? (
              <TopologyEntityLink to={`/dz/links/${selectedItem.data.pk}`}>{selectedItem.data.code}</TopologyEntityLink>
            ) :
            selectedItem.type === 'metro' ? selectedItem.data.name :
            selectedItem.data.votePubkey.substring(0, 12) + '...'
          }
          subtitle={
            selectedItem.type === 'link' ? (
              <span>
                <TopologyEntityLink to={`/dz/devices/${selectedItem.data.deviceAPk}`}>{selectedItem.data.deviceACode}</TopologyEntityLink>
                {' ↔ '}
                <TopologyEntityLink to={`/dz/devices/${selectedItem.data.deviceZPk}`}>{selectedItem.data.deviceZCode}</TopologyEntityLink>
              </span>
            ) : undefined
          }
        >
          {/* eslint-disable @typescript-eslint/no-explicit-any */}
          {selectedItem.type === 'device' && (
            <DeviceDetails device={selectedItem.data as any} />
          )}
          {selectedItem.type === 'link' && (
            <LinkDetails link={selectedItem.data as any} />
          )}
          {selectedItem.type === 'metro' && (
            <MetroDetails metro={selectedItem.data as any} />
          )}
          {selectedItem.type === 'validator' && (
            <ValidatorDetails validator={selectedItem.data as any} />
          )}
          {/* eslint-enable @typescript-eslint/no-explicit-any */}
        </TopologyPanel>
      )}

      {/* Mode panel (right side) - shown when in analysis mode */}
      {panel.isOpen && panel.content === 'mode' && (
        <TopologyPanel
          title={
            mode === 'path' ? 'Path Finding' :
            mode === 'metro-path' ? 'Metro Path Finding' :
            mode === 'whatif-removal' ? 'Simulate Link Removal' :
            mode === 'whatif-addition' ? 'Simulate Link Addition' :
            mode === 'impact' ? 'Device Failure' :
            'Analysis Mode'
          }
          subtitle={
            mode === 'path' ? 'Find shortest paths between two devices by hop count or latency.' :
            mode === 'metro-path' ? 'Find all paths between devices in two metros.' :
            mode === 'whatif-removal' ? 'Analyze what happens to network paths if a link is removed.' :
            mode === 'whatif-addition' ? 'See how adding a new link would improve connectivity.' :
            mode === 'impact' ? 'Analyze the impact of a device failure on network paths.' :
            undefined
          }
        >
          {mode === 'path' && (
            <PathModePanel
              pathSource={pathSource}
              pathTarget={pathTarget}
              pathsResult={pathsResult}
              pathLoading={pathLoading}
              pathMode={pathMode}
              selectedPathIndex={selectedPathIndex}
              devices={deviceOptions}
              showReverse={showReverse}
              reversePathsResult={reversePathsResult}
              reversePathLoading={reversePathLoading}
              selectedReversePathIndex={selectedReversePathIndex}
              onPathModeChange={setPathMode}
              onSelectPath={setSelectedPathIndex}
              onSelectReversePath={setSelectedReversePathIndex}
              onClearPath={clearPath}
              onSetSource={setPathSource}
              onSetTarget={setPathTarget}
              onToggleReverse={() => setShowReverse(prev => !prev)}
            />
          )}
          {mode === 'metro-path' && (
            <MetroPathModePanel
              sourceMetro={metroPathSource}
              targetMetro={metroPathTarget}
              metros={metroOptions}
              pathsResult={metroPathsResult}
              loading={metroPathLoading}
              pathMode={pathMode}
              viewMode={metroPathViewMode}
              selectedPairIndices={metroPathSelectedPairs}
              onSetSourceMetro={setMetroPathSource}
              onSetTargetMetro={setMetroPathTarget}
              onPathModeChange={setPathMode}
              onViewModeChange={setMetroPathViewMode}
              onTogglePair={handleToggleMetroPathPair}
              onClearSelection={() => setMetroPathSelectedPairs([])}
              onClear={() => {
                setMetroPathSource(null)
                setMetroPathTarget(null)
                setMetroPathsResult(null)
                setMetroPathSelectedPairs([])
                setMetroPathViewMode('aggregate')
              }}
            />
          )}
          {mode === 'whatif-removal' && (
            <WhatIfRemovalPanel
              removalLink={removalLink}
              result={removalResult}
              isLoading={removalLoading}
              onClear={() => {
                setRemovalLink(null)
                setRemovalResult(null)
              }}
            />
          )}
          {mode === 'whatif-addition' && (
            <WhatIfAdditionPanel
              additionSource={additionSource}
              additionTarget={additionTarget}
              additionMetric={additionMetric}
              result={additionResult}
              isLoading={additionLoading}
              onMetricChange={setAdditionMetric}
              onClear={() => {
                setAdditionSource(null)
                setAdditionTarget(null)
                setAdditionResult(null)
              }}
            />
          )}
          {mode === 'impact' && (
            <ImpactPanel
              devicePKs={impactDevices}
              deviceCodes={deviceCodeMap}
              result={impactResult}
              isLoading={impactLoading}
              onRemoveDevice={toggleImpactDevice}
              onClear={() => {
                clearImpactDevices()
                setImpactResult(null)
              }}
            />
          )}
        </TopologyPanel>
      )}

      {/* Overlay panel (right side) - shown when overlay is active */}
      {panel.isOpen && panel.content === 'overlay' && (
        <TopologyPanel
          title={
            deviceTypeMode ? 'Device Types' :
            linkTypeMode ? 'Link Types' :
            stakeOverlayMode ? 'Stake' :
            isisHealthMode ? 'ISIS' :
            linkHealthMode ? 'Health' :
            trafficFlowMode ? 'Traffic' :
            criticalityOverlayEnabled ? 'Link Criticality' :
            metroClusteringMode ? 'Metros' :
            showValidators ? 'Validators' :
            (contributorDevicesMode || contributorLinksMode) ? 'Contributors' :
            'Overlay'
          }
          subtitle={
            deviceTypeMode ? 'Devices colored by type (router, switch, etc.).' :
            linkTypeMode ? 'Links colored by type (fiber, wavelength, etc.).' :
            stakeOverlayMode ? 'Devices sized by validator stake.' :
            isisHealthMode ? 'Compare current topology to baseline.' :
            linkHealthMode ? 'Links colored by latency, jitter, and loss.' :
            trafficFlowMode ? 'Links sized by traffic volume.' :
            criticalityOverlayEnabled ? 'Links ranked by impact if removed.' :
            metroClusteringMode ? 'Devices grouped by metro location.' :
            showValidators ? 'Solana validators on the network.' :
            (contributorDevicesMode || contributorLinksMode) ? 'Devices and links by network contributor.' :
            undefined
          }
        >
          {deviceTypeMode && (
            <DeviceTypeOverlayPanel
              isDark={isDark}
              deviceCounts={devices.reduce((acc, d) => {
                const type = d.device_type?.toLowerCase() || 'unknown'
                acc[type] = (acc[type] || 0) + 1
                return acc
              }, {} as Record<string, number>)}
            />
          )}
          {linkTypeMode && (
            <LinkTypeOverlayPanel
              isDark={isDark}
              linkCounts={links.reduce((acc, l) => {
                const type = l.link_type || 'unknown'
                acc[type] = (acc[type] || 0) + 1
                return acc
              }, {} as Record<string, number>)}
            />
          )}
          {stakeOverlayMode && (
            <StakeOverlayPanel
              deviceStakeMap={deviceStakeMap}
              getStakeColor={getStakeColor}
              getDeviceLabel={(pk) => deviceMap.get(pk)?.code || pk.substring(0, 8)}
              isLoading={devices.length === 0}
            />
          )}
          {isisHealthMode && (
            <ComparePanel
              data={compareData ?? null}
              isLoading={compareLoading}
            />
          )}
          {linkHealthMode && (
            <LinkHealthOverlayPanel
              linkHealthData={linkHealthData}
              isLoading={!linkHealthData}
            />
          )}
          {trafficFlowMode && (
            <TrafficFlowOverlayPanel
              edgeTrafficMap={edgeTrafficMap}
              links={links}
              isLoading={links.length === 0}
            />
          )}
          {criticalityOverlayEnabled && (
            <CriticalityPanel
              data={criticalLinksData ?? null}
              isLoading={!criticalLinksData}
            />
          )}
          {metroClusteringMode && (
            <MetroClusteringOverlayPanel
              metroInfoMap={metroInfoMap}
              collapsedMetros={collapsedMetros}
              getMetroColor={(pk) => getMetroColor(pk, metroIndexMap.get(pk) ?? 0)}
              getDeviceCountForMetro={(pk) => devicesByMetro.get(pk)?.length ?? 0}
              totalDeviceCount={devices.length}
              onToggleMetroCollapse={toggleMetroCollapse}
              onCollapseAll={() => setCollapsedMetros(new Set(metroInfoMap.keys()))}
              onExpandAll={() => setCollapsedMetros(new Set())}
              isLoading={metros.length === 0}
            />
          )}
          {showValidators && (
            <ValidatorsOverlayPanel
              validators={validators}
              isLoading={validators.length === 0}
            />
          )}
          {(contributorDevicesMode || contributorLinksMode) && (
            <ContributorsOverlayPanel
              contributorInfoMap={contributorInfoMap}
              getContributorColor={(pk) => getContributorColor(pk, contributorIndexMap.get(pk) ?? 0)}
              getDeviceCountForContributor={(pk) => devicesByContributor.get(pk)?.length ?? 0}
              getLinkCountForContributor={(pk) => linksByContributor.get(pk)?.length ?? 0}
              totalDeviceCount={devices.length}
              totalLinkCount={links.length}
              isLoading={devices.length === 0}
            />
          )}
          {multicastTreesMode && (
            <MulticastTreesOverlayPanel
              isDark={isDark}
              selectedGroup={selectedMulticastGroup}
              onSelectGroup={handleSelectMulticastGroup}
              groupDetails={multicastGroupDetails}
              enabledPublishers={enabledPublishers}
              enabledSubscribers={enabledSubscribers}
              onTogglePublisher={handleTogglePublisher}
              onToggleSubscriber={handleToggleSubscriber}
              publisherColorMap={multicastPublisherColorMap}
              dimOtherLinks={dimOtherLinks}
              onToggleDimOtherLinks={() => setDimOtherLinks(prev => !prev)}
              animateFlow={animateFlow}
              onToggleAnimateFlow={() => setAnimateFlow(prev => !prev)}
              validators={validators}
              showTreeValidators={showTreeValidators}
              onToggleShowTreeValidators={() => setShowTreeValidators(prev => !prev)}
            />
          )}
        </TopologyPanel>
      )}
    </>
  )
}
