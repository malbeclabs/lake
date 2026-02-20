import { useMemo, useEffect, useRef, useState, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useTheme } from '@/hooks/use-theme'
import Globe from 'react-globe.gl'
import type { GlobeInstance } from 'react-globe.gl'
import { useQuery } from '@tanstack/react-query'
import type { TopologyMetro, TopologyDevice, TopologyLink, TopologyValidator, MultiPathResponse, SimulateLinkRemovalResponse, SimulateLinkAdditionResponse, WhatIfRemovalResponse, MetroDevicePathsResponse, MulticastGroupDetail, MulticastTreeResponse } from '@/lib/api'
import { fetchISISPaths, fetchISISTopology, fetchCriticalLinks, fetchSimulateLinkRemoval, fetchSimulateLinkAddition, fetchWhatIfRemoval, fetchLinkHealth, fetchTopologyCompare, fetchMetroDevicePaths, fetchMulticastGroup, fetchMulticastTreePaths } from '@/lib/api'
import { useTopology, TopologyControlBar, TopologyPanel, DeviceDetails, LinkDetails, MetroDetails, ValidatorDetails, EntityLink as TopologyEntityLink, PathModePanel, MetroPathModePanel, CriticalityPanel, WhatIfRemovalPanel, WhatIfAdditionPanel, ImpactPanel, ComparePanel, StakeOverlayPanel, LinkHealthOverlayPanel, TrafficFlowOverlayPanel, MetroClusteringOverlayPanel, ContributorsOverlayPanel, ValidatorsOverlayPanel, DeviceTypeOverlayPanel, LinkTypeOverlayPanel, MulticastTreesOverlayPanel, LINK_TYPE_COLORS, MULTICAST_PUBLISHER_COLORS, type DeviceOption, type MetroOption } from '@/components/topology'
import type { LinkInfo, SelectedItemData } from '@/components/topology'
import { formatBandwidth, formatTrafficRate } from '@/components/topology'

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
const DEVICE_TYPE_COLORS: Record<string, string> = {
  hybrid: '#eab308',    // yellow
  transit: '#f97316',   // orange
  edge: '#22d3ee',      // cyan
  default: '#9ca3af',   // gray
}

interface TopologyGlobeProps {
  metros: TopologyMetro[]
  devices: TopologyDevice[]
  links: TopologyLink[]
  validators: TopologyValidator[]
}

// Globe point entity (devices, metros, validators)
interface GlobePointDevice {
  entityType: 'device'
  pk: string
  code: string
  deviceType: string
  status: string
  lat: number
  lng: number
  metroPk: string
  metroName: string
  contributorPk: string
  contributorCode: string
  stakeSol: number
  stakeShare: number
  userCount: number
  validatorCount: number
  interfaces: { name: string; ip: string; status: string }[]
}

interface GlobePointMetro {
  entityType: 'metro'
  pk: string
  code: string
  name: string
  lat: number
  lng: number
  deviceCount: number
}

interface GlobePointValidator {
  entityType: 'validator'
  votePubkey: string
  nodePubkey: string
  tunnelId: number
  devicePk: string
  lat: number
  lng: number
  stakeSol: number
  stakeShare: number
  city: string
  country: string
  commission: number
  version: string
  gossipIp: string
  gossipPort: number
  tpuQuicIp: string
  tpuQuicPort: number
  deviceCode: string
  metroPk: string
  metroName: string
  inBps: number
  outBps: number
}

type GlobePointEntity = GlobePointDevice | GlobePointMetro | GlobePointValidator

// Globe arc entity (links, validator connecting lines)
interface GlobeArcLink {
  entityType: 'link'
  pk: string
  code: string
  linkType: string
  startLat: number
  startLng: number
  endLat: number
  endLng: number
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
}

interface GlobeArcValidatorLink {
  entityType: 'validator-link'
  votePubkey: string
  devicePk: string
  role: 'pub' | 'sub' | null
  startLat: number
  startLng: number
  endLat: number
  endLng: number
}

interface GlobeArcInterMetro {
  entityType: 'inter-metro'
  metroAPk: string
  metroACode: string
  metroZPk: string
  metroZCode: string
  linkCount: number
  avgLatencyUs: number
  startLat: number
  startLng: number
  endLat: number
  endLng: number
}

interface GlobeArcMulticastTree {
  entityType: 'multicast-tree'
  publisherPK: string
  colorIndex: number
  startLat: number
  startLng: number
  endLat: number
  endLng: number
  altitude: number
}

type GlobeArcEntity = GlobeArcLink | GlobeArcValidatorLink | GlobeArcInterMetro | GlobeArcMulticastTree

// Get per-publisher arc altitude for parallel multicast tree arcs.
// Scales with great-circle distance so long arcs don't clip through the globe.
function getPublisherAltitude(
  publisherIndex: number,
  totalPublishers: number,
  startLat: number, startLng: number,
  endLat: number, endLng: number,
): number {
  // Great-circle angular distance (radians)
  const toRad = Math.PI / 180
  const lat1 = startLat * toRad, lat2 = endLat * toRad
  const dLng = (endLng - startLng) * toRad
  const angularDist = Math.acos(
    Math.min(1, Math.max(-1,
      Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(dLng)
    ))
  )
  // Scale with distance, with a minimum for short arcs
  const base = Math.max(0.06, angularDist * 0.15)
  if (totalPublishers === 1) return base
  const spread = 0.04
  const start = base - (spread * (totalPublishers - 1)) / 2
  return start + spread * publisherIndex
}

// Get link weight based on bandwidth capacity
function getBandwidthStroke(bps: number): number {
  const gbps = bps / 1e9
  if (gbps >= 100) return 1.2
  if (gbps >= 40) return 0.8
  if (gbps >= 10) return 0.5
  if (gbps >= 1) return 0.35
  return 0.3
}

// Calculate validator globe radius based on stake (logarithmic scale)
function calculateValidatorGlobeRadius(stakeSol: number): number {
  if (stakeSol <= 0) return 0.08
  const minRadius = 0.08
  const maxRadius = 0.25
  const minStake = 1000
  const radius = minRadius + Math.log10(Math.max(minStake, stakeSol) / minStake) * 0.04
  return Math.min(maxRadius, radius)
}

// Calculate device stake size for globe
function calculateDeviceStakeGlobeRadius(stakeSol: number): number {
  if (stakeSol <= 0) return 0.12
  const minRadius = 0.12
  const maxRadius = 0.45
  const minStake = 10000
  const radius = minRadius + Math.log10(Math.max(minStake, stakeSol) / minStake) * 0.08
  return Math.min(maxRadius, radius)
}

// Get stake-based color intensity
function getStakeColor(stakeShare: number): string {
  if (stakeShare <= 0) return '#6b7280'
  const t = Math.min(stakeShare / 1.0, 1)
  const r = Math.round(234 + t * (234 - 234))
  const g = Math.round(179 - t * (179 - 88))
  const b = Math.round(8 + t * (8 - 8))
  return `rgb(${r}, ${g}, ${b})`
}

// Get traffic color based on utilization
function getTrafficColor(link: TopologyLink): { color: string; stroke: number } {
  const totalBps = (link.in_bps ?? 0) + (link.out_bps ?? 0)
  const utilization = link.bandwidth_bps > 0 ? (totalBps / link.bandwidth_bps) * 100 : 0

  if (utilization >= 80) return { color: '#ef4444', stroke: 1.0 }
  if (utilization >= 50) return { color: '#eab308', stroke: 0.8 }
  if (utilization >= 20) return { color: '#84cc16', stroke: 0.6 }
  if (totalBps > 0) return { color: '#22c55e', stroke: 0.5 }
  return { color: '#6b7280', stroke: 0.3 }
}

// Arc dash animation speed based on avg latency
function arcAnimateTime(avgLatencyUs: number): number {
  // 0 or missing latency = unknown; use a calm default
  if (!avgLatencyUs || avgLatencyUs <= 0) return 2000
  const latencyMs = avgLatencyUs / 1000
  const clamped = Math.max(1, Math.min(200, latencyMs))
  // Low latency (1ms) → 1200ms cycle (fast), high latency (200ms) → 3600ms cycle (slow)
  return 1200 + (Math.log(clamped) / Math.log(200)) * 2400
}

// Calculate device position with radial offset around metro center
function calculateDevicePosition(
  metroLat: number,
  metroLng: number,
  deviceIndex: number,
  totalDevices: number
): { lat: number; lng: number } {
  if (totalDevices === 1) {
    return { lat: metroLat, lng: metroLng }
  }
  const radius = 0.3
  const angle = (2 * Math.PI * deviceIndex) / totalDevices
  const latOffset = radius * Math.cos(angle)
  const lngOffset = radius * Math.sin(angle) / Math.cos(metroLat * Math.PI / 180)
  return { lat: metroLat + latOffset, lng: metroLng + lngOffset }
}

export function TopologyGlobe({ metros, devices, links, validators }: TopologyGlobeProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const globeRef = useRef<GlobeInstance | null>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 })

  // Preload all globe textures so theme switching doesn't need a network fetch.
  const [texturesLoaded, setTexturesLoaded] = useState(false)
  useEffect(() => {
    const urls = ['/textures/earth-day.jpg', '/textures/earth-night.jpg', '/textures/night-sky.jpg']
    let loaded = 0
    for (const url of urls) {
      const img = new Image()
      img.onload = img.onerror = () => { if (++loaded >= urls.length) setTexturesLoaded(true) }
      img.src = url
    }
  }, [])

  // Show loading overlay during theme transitions while globe re-renders.
  const prevThemeRef = useRef(isDark)
  const [themeSwitching, setThemeSwitching] = useState(false)
  useEffect(() => {
    if (prevThemeRef.current === isDark) return
    prevThemeRef.current = isDark
    setThemeSwitching(true)
    // Give globe time to re-render with new texture
    const timer = setTimeout(() => setThemeSwitching(false), 1500)
    return () => clearTimeout(timer)
  }, [isDark])

  const globeImageUrl = isDark ? '/textures/earth-night.jpg' : '/textures/earth-day.jpg'

  const [autoRotateEnabled, setAutoRotateEnabled] = useState(true) // user preference
  const [autoRotating, setAutoRotating] = useState(true) // actual state
  const [linkAnimating, setLinkAnimating] = useState(true) // link animation state
  const [searchParams, setSearchParams] = useSearchParams()
  const [selectedItem, setSelectedItemState] = useState<SelectedItemData | null>(null)
  const settingSelectionLocallyRef = useRef(false)

  // Get unified topology context
  const { mode, setMode, pathMode, setPathMode, overlays, toggleOverlay, panel, openPanel, closePanel, selection, impactDevices, toggleImpactDevice, clearImpactDevices } = useTopology()

  // Derive mode states from context
  const pathModeEnabled = mode === 'path'
  const metroPathModeEnabled = mode === 'metro-path'
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

  // True when user is actively analyzing — any overlay, mode, or selection active.
  // Used to stop arc animation and show solid lines for clarity.
  // Bandwidth is excluded because it's always-on sizing, not an analytical overlay.
  const anyOverlayActive = Object.entries(overlays).some(([key, value]) => key !== 'bandwidth' && value)
  const isAnalysisActive = anyOverlayActive || mode !== 'explore' || !!selectedItem

  // Multicast trees operational state (local)
  const [selectedMulticastGroup, setSelectedMulticastGroup] = useState<string | null>(null)
  const [multicastGroupDetails, setMulticastGroupDetails] = useState<Map<string, MulticastGroupDetail>>(new Map())
  const [multicastTreePaths, setMulticastTreePaths] = useState<Map<string, MulticastTreeResponse>>(new Map())
  const [enabledPublishers, setEnabledPublishers] = useState<Set<string>>(new Set())
  const [enabledSubscribers, setEnabledSubscribers] = useState<Set<string>>(new Set())
  const [dimOtherLinks, setDimOtherLinks] = useState(true)
  const [animateFlow, setAnimateFlow] = useState(true)
  const [showTreeValidators, setShowTreeValidators] = useState(true)
  // PKs to skip during auto-enable (restored from URL on initial load)
  const initialDisabledPubsRef = useRef<Set<string> | null>(null)
  const initialDisabledSubsRef = useRef<Set<string> | null>(null)


  // Multicast handlers
  const handleSelectMulticastGroup = useCallback((code: string | null) => {
    setSelectedMulticastGroup(code)
  }, [])

  const handleTogglePublisher = useCallback((devicePK: string) => {
    setEnabledPublishers(prev => {
      const next = new Set(prev)
      if (next.has(devicePK)) next.delete(devicePK)
      else next.add(devicePK)
      return next
    })
  }, [])

  const handleToggleSubscriber = useCallback((devicePK: string) => {
    setEnabledSubscribers(prev => {
      const next = new Set(prev)
      if (next.has(devicePK)) next.delete(devicePK)
      else next.add(devicePK)
      return next
    })
  }, [])

  // Handler to select/deselect all publishers
  const handleSetAllPublishers = useCallback((enabled: boolean) => {
    if (!selectedMulticastGroup) return
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return
    if (enabled) {
      const pubs = new Set<string>()
      detail.members.forEach(m => {
        if (m.mode === 'P' || m.mode === 'P+S') pubs.add(m.device_pk)
      })
      setEnabledPublishers(pubs)
    } else {
      setEnabledPublishers(new Set())
    }
  }, [selectedMulticastGroup, multicastGroupDetails])

  // Handler to select/deselect all subscribers
  const handleSetAllSubscribers = useCallback((enabled: boolean) => {
    if (!selectedMulticastGroup) return
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return
    if (enabled) {
      const subs = new Set<string>()
      detail.members.forEach(m => {
        if (m.mode === 'S' || m.mode === 'P+S') subs.add(m.device_pk)
      })
      setEnabledSubscribers(subs)
    } else {
      setEnabledSubscribers(new Set())
    }
  }, [selectedMulticastGroup, multicastGroupDetails])

  // Fetch multicast tree paths when group is selected
  useEffect(() => {
    if (!multicastTreesMode || !selectedMulticastGroup) return
    if (multicastTreePaths.has(selectedMulticastGroup)) return
    const code = selectedMulticastGroup
    fetchMulticastTreePaths(code)
      .then(result => setMulticastTreePaths(prev => new Map(prev).set(code, result)))
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

  // Set enabled publishers/subscribers when group details load
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

  const handleToggleMetroPathPair = useCallback((index: number) => {
    setMetroPathSelectedPairs(prev => {
      if (prev.includes(index)) return prev.filter(i => i !== index)
      if (prev.length >= 5) return prev
      return [...prev, index]
    })
  }, [])

  // What-If states
  const [removalLink, setRemovalLink] = useState<{ sourcePK: string; targetPK: string; linkPK: string } | null>(null)
  const [removalResult, setRemovalResult] = useState<SimulateLinkRemovalResponse | null>(null)
  const [removalLoading, setRemovalLoading] = useState(false)
  const [additionSource, setAdditionSource] = useState<string | null>(null)
  const [additionTarget, setAdditionTarget] = useState<string | null>(null)
  const [additionMetric, setAdditionMetric] = useState<number>(1000)
  const [additionResult, setAdditionResult] = useState<SimulateLinkAdditionResponse | null>(null)
  const [additionLoading, setAdditionLoading] = useState(false)
  const [impactResult, setImpactResult] = useState<WhatIfRemovalResponse | null>(null)
  const [impactLoading, setImpactLoading] = useState(false)

  // Metro clustering state
  const [collapsedMetros, setCollapsedMetros] = useState<Set<string>>(new Set())

  // ─── Data-fetching queries ───────────────────────────────────────────

  const { data: isisTopology } = useQuery({
    queryKey: ['isis-topology'],
    queryFn: fetchISISTopology,
    enabled: pathModeEnabled,
  })

  const isisDevicePKs = useMemo(() => {
    if (!isisTopology?.nodes) return new Set<string>()
    return new Set(isisTopology.nodes.map(node => node.data.id))
  }, [isisTopology])

  const { data: criticalLinksData } = useQuery({
    queryKey: ['critical-links'],
    queryFn: fetchCriticalLinks,
    enabled: criticalityOverlayEnabled,
  })

  const { data: linkHealthData } = useQuery({
    queryKey: ['link-health'],
    queryFn: fetchLinkHealth,
    enabled: linkHealthMode,
    staleTime: 30000,
  })

  const { data: compareData, isLoading: compareLoading } = useQuery({
    queryKey: ['topology-compare'],
    queryFn: fetchTopologyCompare,
    enabled: isisHealthMode,
    refetchInterval: 60000,
  })

  // ─── Derived lookup maps ─────────────────────────────────────────────

  const metroMap = useMemo(() => {
    const map = new Map<string, TopologyMetro>()
    for (const m of metros) map.set(m.pk, m)
    return map
  }, [metros])

  const metroIndexMap = useMemo(() => {
    const map = new Map<string, number>()
    const sorted = [...metros].sort((a, b) => a.code.localeCompare(b.code))
    sorted.forEach((m, i) => map.set(m.pk, i))
    return map
  }, [metros])

  const deviceMap = useMemo(() => {
    const map = new Map<string, TopologyDevice>()
    for (const d of devices) map.set(d.pk, d)
    return map
  }, [devices])

  const deviceCodeMap = useMemo(() => {
    const map = new Map<string, string>()
    for (const d of devices) map.set(d.pk, d.code)
    return map
  }, [devices])

  const deviceOptions: DeviceOption[] = useMemo(() => {
    return devices
      .map(d => {
        const metro = metros.find(m => m.pk === d.metro_pk)
        return { pk: d.pk, code: d.code, deviceType: d.device_type, metro: metro?.code }
      })
      .sort((a, b) => a.code.localeCompare(b.code))
  }, [devices, metros])

  const metroOptions: MetroOption[] = useMemo(() => {
    return metros.map(m => ({ pk: m.pk, code: m.code, name: m.name })).sort((a, b) => a.code.localeCompare(b.code))
  }, [metros])

  const linkMap = useMemo(() => {
    const map = new Map<string, TopologyLink>()
    for (const l of links) map.set(l.pk, l)
    return map
  }, [links])

  const validatorMap = useMemo(() => {
    const map = new Map<string, TopologyValidator>()
    for (const v of validators) map.set(v.vote_pubkey, v)
    return map
  }, [validators])

  const devicesByMetro = useMemo(() => {
    const map = new Map<string, TopologyDevice[]>()
    for (const d of devices) {
      const list = map.get(d.metro_pk) || []
      list.push(d)
      map.set(d.metro_pk, list)
    }
    return map
  }, [devices])

  const contributorIndexMap = useMemo(() => {
    const map = new Map<string, number>()
    const contributorSet = new Map<string, string>()
    for (const d of devices) {
      if (d.contributor_pk) contributorSet.set(d.contributor_pk, d.contributor_code || d.contributor_pk)
    }
    const sorted = [...contributorSet.entries()].sort((a, b) => a[1].localeCompare(b[1]))
    sorted.forEach(([pk], i) => map.set(pk, i))
    return map
  }, [devices])

  const devicesByContributor = useMemo(() => {
    const map = new Map<string, TopologyDevice[]>()
    for (const d of devices) {
      if (d.contributor_pk) {
        const list = map.get(d.contributor_pk) || []
        list.push(d)
        map.set(d.contributor_pk, list)
      }
    }
    return map
  }, [devices])

  const linksByContributor = useMemo(() => {
    const map = new Map<string, TopologyLink[]>()
    for (const l of links) {
      if (l.contributor_pk) {
        const list = map.get(l.contributor_pk) || []
        list.push(l)
        map.set(l.contributor_pk, list)
      }
    }
    return map
  }, [links])

  const contributorInfoMap = useMemo(() => {
    const map = new Map<string, { code: string; name: string }>()
    for (const d of devices) {
      if (d.contributor_pk && !map.has(d.contributor_pk)) {
        map.set(d.contributor_pk, {
          code: d.contributor_code || d.contributor_pk.substring(0, 8),
          name: d.contributor_code || '',
        })
      }
    }
    return map
  }, [devices])

  const deviceStakeMap = useMemo(() => {
    const map = new Map<string, { stakeSol: number; validatorCount: number }>()
    for (const d of devices) map.set(d.pk, { stakeSol: d.stake_sol ?? 0, validatorCount: d.validator_count ?? 0 })
    return map
  }, [devices])

  const metroInfoMap = useMemo(() => {
    const map = new Map<string, { code: string; name: string }>()
    for (const m of metros) map.set(m.pk, { code: m.code, name: m.name })
    return map
  }, [metros])

  const edgeTrafficMap = useMemo(() => {
    const map = new Map<string, { inBps: number; outBps: number; bandwidthBps: number; utilization: number }>()
    for (const l of links) {
      const inBps = l.in_bps ?? 0
      const outBps = l.out_bps ?? 0
      const bw = l.bandwidth_bps ?? 0
      map.set(l.pk, { inBps, outBps, bandwidthBps: bw, utilization: bw > 0 ? Math.max(inBps, outBps) / bw : 0 })
    }
    return map
  }, [links])

  // Device positions
  const devicePositions = useMemo(() => {
    const positions = new Map<string, { lat: number; lng: number }>()
    for (const [metroPk, metroDevices] of devicesByMetro) {
      const metro = metroMap.get(metroPk)
      if (!metro) continue
      metroDevices.forEach((device, index) => {
        positions.set(device.pk, calculateDevicePosition(metro.latitude, metro.longitude, index, metroDevices.length))
      })
    }
    return positions
  }, [devicesByMetro, metroMap])

  // ─── Derived overlay maps ────────────────────────────────────────────

  const linkSlaStatus = useMemo(() => {
    if (!linkHealthData?.links) return new Map<string, { status: string; avgRttUs: number; committedRttNs: number; lossPct: number; slaRatio: number }>()
    const slaMap = new Map<string, { status: string; avgRttUs: number; committedRttNs: number; lossPct: number; slaRatio: number }>()
    for (const l of linkHealthData.links) {
      slaMap.set(l.link_pk, { status: l.sla_status, avgRttUs: l.avg_rtt_us, committedRttNs: l.committed_rtt_ns, lossPct: l.loss_pct, slaRatio: l.sla_ratio })
    }
    return slaMap
  }, [linkHealthData])

  const edgeHealthStatus = useMemo(() => {
    if (!compareData?.discrepancies) return new Map<string, string>()
    const status = new Map<string, string>()
    for (const d of compareData.discrepancies) {
      const key1 = `${d.deviceAPK}|${d.deviceBPK}`
      const key2 = `${d.deviceBPK}|${d.deviceAPK}`
      const type = d.type === 'missing_isis' ? 'missing' : d.type === 'extra_isis' ? 'extra' : 'mismatch'
      status.set(key1, type)
      status.set(key2, type)
    }
    return status
  }, [compareData])

  const linkCriticalityMap = useMemo(() => {
    const map = new Map<string, 'critical' | 'important' | 'redundant'>()
    if (!criticalLinksData?.links) return map
    const devicePairToLinkPK = new Map<string, string>()
    for (const l of links) {
      devicePairToLinkPK.set(`${l.side_a_pk}|${l.side_z_pk}`, l.pk)
      devicePairToLinkPK.set(`${l.side_z_pk}|${l.side_a_pk}`, l.pk)
    }
    for (const cl of criticalLinksData.links) {
      const linkPK = devicePairToLinkPK.get(`${cl.sourcePK}|${cl.targetPK}`)
      if (linkPK) map.set(linkPK, cl.criticality)
    }
    return map
  }, [criticalLinksData, links])

  const disconnectedDevicePKs = useMemo(() => {
    const set = new Set<string>()
    if (removalResult?.disconnectedDevices) removalResult.disconnectedDevices.forEach(d => set.add(d.pk))
    return set
  }, [removalResult])

  // ─── Path maps ───────────────────────────────────────────────────────

  const devicePathMap = useMemo(() => {
    const map = new Map<string, number[]>()
    if (!pathsResult?.paths?.length) return map
    pathsResult.paths.forEach((path, pi) => {
      path.path.forEach(hop => {
        const existing = map.get(hop.devicePK) || []
        if (!existing.includes(pi)) existing.push(pi)
        map.set(hop.devicePK, existing)
      })
    })
    return map
  }, [pathsResult])

  const linkPathMap = useMemo(() => {
    const map = new Map<string, number[]>()
    if (!pathsResult?.paths?.length) return map
    pathsResult.paths.forEach((singlePath, pi) => {
      for (let i = 0; i < singlePath.path.length - 1; i++) {
        const fromPK = singlePath.path[i].devicePK
        const toPK = singlePath.path[i + 1].devicePK
        for (const link of links) {
          if ((link.side_a_pk === fromPK && link.side_z_pk === toPK) || (link.side_a_pk === toPK && link.side_z_pk === fromPK)) {
            const existing = map.get(link.pk) || []
            if (!existing.includes(pi)) existing.push(pi)
            map.set(link.pk, existing)
            break
          }
        }
      }
    })
    return map
  }, [pathsResult, links])

  const metroDevicePathMap = useMemo(() => {
    const map = new Map<string, number[]>()
    if (!metroPathsResult?.devicePairs?.length) return map
    metroPathsResult.devicePairs.forEach((pair, pi) => {
      pair.bestPath?.path?.forEach(hop => {
        const existing = map.get(hop.devicePK) || []
        if (!existing.includes(pi)) existing.push(pi)
        map.set(hop.devicePK, existing)
      })
    })
    return map
  }, [metroPathsResult])

  const metroLinkPathMap = useMemo(() => {
    const map = new Map<string, number[]>()
    if (!metroPathsResult?.devicePairs?.length) return map
    metroPathsResult.devicePairs.forEach((pair, pi) => {
      const path = pair.bestPath?.path
      if (!path?.length) return
      for (let i = 0; i < path.length - 1; i++) {
        const fromPK = path[i].devicePK
        const toPK = path[i + 1].devicePK
        for (const link of links) {
          if ((link.side_a_pk === fromPK && link.side_z_pk === toPK) || (link.side_a_pk === toPK && link.side_z_pk === fromPK)) {
            const existing = map.get(link.pk) || []
            if (!existing.includes(pi)) existing.push(pi)
            map.set(link.pk, existing)
            break
          }
        }
      }
    })
    return map
  }, [metroPathsResult, links])

  // ─── Multicast path maps ─────────────────────────────────────────────

  // Set of device PKs in any multicast tree path (for device coloring)
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

  // Per-publisher tree segments with device PKs and positions (for multicast-tree arcs)
  const multicastPublisherPaths = useMemo(() => {
    const result: Array<{ publisherPK: string; segments: Array<{ fromPK: string; toPK: string; fromLat: number; fromLng: number; toLat: number; toLng: number }> }> = []
    if (!multicastTreesMode || !selectedMulticastGroup) return result

    const publisherSegments = new Map<string, Map<string, { fromPK: string; toPK: string; fromLat: number; fromLng: number; toLat: number; toLng: number }>>()

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
            segs.set(key, { fromPK, toPK, fromLat: fromPos.lat, fromLng: fromPos.lng, toLat: toPos.lat, toLng: toPos.lng })
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

  // Map from canonical segment key -> ordered publisher PKs (for altitude offset calculation)
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

  const multicastPublisherDevices = useMemo(() => {
    const set = new Set<string>()
    if (!multicastTreesMode) return set
    if (selectedMulticastGroup) {
      const code = selectedMulticastGroup
      const detail = multicastGroupDetails.get(code)
      if (detail?.members) {
        detail.members.filter(m => m.mode === 'P' || m.mode === 'P+S').filter(m => enabledPublishers.has(m.device_pk)).forEach(m => set.add(m.device_pk))
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
        detail.members.filter(m => m.mode === 'S' || m.mode === 'P+S').filter(m => enabledSubscribers.has(m.device_pk)).forEach(m => set.add(m.device_pk))
      }
    }
    return set
  }, [multicastTreesMode, selectedMulticastGroup, multicastGroupDetails, enabledSubscribers])

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

  // ─── Selection management ────────────────────────────────────────────

  const setSelectedItem = useCallback((item: SelectedItemData | null) => {
    settingSelectionLocallyRef.current = true
    setSelectedItemState(item)
    if (item === null) {
      setSearchParams({})
      if (panel.content === 'details') closePanel()
    } else {
      const params: Record<string, string> = { type: item.type }
      if (item.type === 'validator') params.id = item.data.votePubkey
      else if (item.type === 'device') params.id = item.data.pk
      else if (item.type === 'link') params.id = item.data.pk
      else if (item.type === 'metro') params.id = item.data.pk
      setSearchParams(params)
      if (mode === 'explore') openPanel('details')
    }
  }, [setSearchParams, panel.content, closePanel, mode, openPanel])

  // Sync context selection to local state
  useEffect(() => {
    if (settingSelectionLocallyRef.current) {
      settingSelectionLocallyRef.current = false
      return
    }
    if (selection === null && selectedItem !== null) setSelectedItemState(null)
  }, [selection, selectedItem])

  // Build link info for panel/hover
  const buildLinkInfo = useCallback((link: TopologyLink): LinkInfo => {
    const healthInfo = linkSlaStatus.get(link.pk)
    return {
      pk: link.pk, code: link.code, linkType: link.link_type,
      bandwidthBps: link.bandwidth_bps, latencyUs: link.latency_us,
      jitterUs: link.jitter_us ?? 0, latencyAtoZUs: link.latency_a_to_z_us,
      jitterAtoZUs: link.jitter_a_to_z_us ?? 0, latencyZtoAUs: link.latency_z_to_a_us,
      jitterZtoAUs: link.jitter_z_to_a_us ?? 0, lossPercent: link.loss_percent ?? 0,
      inBps: link.in_bps, outBps: link.out_bps,
      deviceAPk: link.side_a_pk, deviceACode: link.side_a_code || 'Unknown',
      interfaceAName: link.side_a_iface_name || '', interfaceAIP: link.side_a_ip || '',
      deviceZPk: link.side_z_pk, deviceZCode: link.side_z_code || 'Unknown',
      interfaceZName: link.side_z_iface_name || '', interfaceZIP: link.side_z_ip || '',
      contributorPk: link.contributor_pk, contributorCode: link.contributor_code,
      sampleCount: link.sample_count ?? 0, committedRttNs: link.committed_rtt_ns,
      isisDelayOverrideNs: link.isis_delay_override_ns,
      health: healthInfo ? { status: healthInfo.status, committedRttNs: healthInfo.committedRttNs, slaRatio: healthInfo.slaRatio, lossPct: healthInfo.lossPct } : undefined,
    }
  }, [linkSlaStatus])

  // ─── Dimension tracking ──────────────────────────────────────────────

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (entry) setDimensions({ width: entry.contentRect.width, height: entry.contentRect.height })
    })
    observer.observe(el)
    setDimensions({ width: el.clientWidth, height: el.clientHeight })
    return () => observer.disconnect()
  }, [])

  // ─── Auto-rotation ───────────────────────────────────────────────────

  const setAutoRotate = useCallback((enabled: boolean) => {
    const globe = globeRef.current
    if (!globe) return
    const controls = globe.controls()
    controls.autoRotate = enabled
    controls.autoRotateSpeed = 0.3
    setAutoRotating(enabled)
  }, [])

  // Callback ref: fires when the Globe instance mounts. Set the initial
  // camera position immediately so animate-in zooms into the right area.
  const initialFlyDone = useRef(false)
  const [globeReady, setGlobeReady] = useState(false)
  const globeRefCb = useCallback((instance: GlobeInstance | null) => {
    globeRef.current = instance
    if (!instance || initialFlyDone.current) return
    initialFlyDone.current = true
    // Set initial position immediately (no transition) so the animate-in
    // zoom happens over North Atlantic instead of the default { lat:0, lng:0 }
    instance.pointOfView({ lat: 35, lng: -30, altitude: 2.2 }, 0)
    // Signal that globe is mounted; the rotation effect below will decide
    // whether to start rotating based on isAnalysisActive.
    setTimeout(() => setGlobeReady(true), 1000)
  }, [])

  // Pause/resume rotation on transitions in/out of analysis mode.
  // Only fires on state changes, so explicit user play/pause clicks
  // are not overridden by the effect.
  const prevAnalysisActive = useRef<boolean | null>(null) // null = first run
  useEffect(() => {
    if (!globeReady) return
    const isFirst = prevAnalysisActive.current === null
    const wasActive = prevAnalysisActive.current
    prevAnalysisActive.current = isAnalysisActive
    if (isFirst) {
      // Initial mount: respect current state
      if (isAnalysisActive) {
        setAutoRotate(false)
      } else if (autoRotateEnabled) {
        setAutoRotate(true)
      }
    } else {
      const entering = isAnalysisActive && !wasActive
      const leaving = !isAnalysisActive && wasActive
      if (entering) {
        setAutoRotate(false)
      } else if (leaving && autoRotateEnabled) {
        setAutoRotate(true)
      }
    }
  }, [globeReady, isAnalysisActive, autoRotateEnabled, setAutoRotate])

  // Auto-disable link animation on transitions in/out of analysis mode.
  // User can re-enable via the toggle even while in analysis mode.
  const prevAnalysisForLinks = useRef<boolean | null>(null)
  useEffect(() => {
    if (!globeReady) return
    const isFirst = prevAnalysisForLinks.current === null
    const wasActive = prevAnalysisForLinks.current
    prevAnalysisForLinks.current = isAnalysisActive
    if (isFirst) {
      if (isAnalysisActive) setLinkAnimating(false)
    } else {
      if (isAnalysisActive && !wasActive) setLinkAnimating(false)
      if (!isAnalysisActive && wasActive) setLinkAnimating(true)
    }
  }, [globeReady, isAnalysisActive])

  // Pause auto-rotation during direct globe interaction (drag/scroll on the canvas),
  // then resume after 3s of inactivity. Brief clicks don't affect rotation — entity
  // selection already pauses via isAnalysisActive.
  const interactionTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined)
  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    const pauseAndDebounce = (e: Event) => {
      if ((e.target as HTMLElement).tagName !== 'CANVAS') return
      const globe = globeRef.current
      if (!globe) return
      // Pause rotation during interaction
      globe.controls().autoRotate = false
      setAutoRotating(false)
      // Clear any pending resume timer
      clearTimeout(interactionTimerRef.current)
      // Resume after 3s of no interaction (if user preference is enabled and no analysis active)
      interactionTimerRef.current = setTimeout(() => {
        if (!autoRotateEnabled || isAnalysisActive) return
        setAutoRotate(true)
      }, 3000)
    }

    // Only drag and zoom should pause — not pointerdown (which fires on brief clicks too).
    // OrbitControls uses pointermove after pointerdown for drag, so listen to that.
    let pointerIsDown = false
    const onPointerDown = (e: Event) => {
      if ((e.target as HTMLElement).tagName !== 'CANVAS') return
      pointerIsDown = true
    }
    const onPointerMove = (e: Event) => {
      if (!pointerIsDown) return
      pauseAndDebounce(e)
    }
    const onPointerUp = () => { pointerIsDown = false }

    el.addEventListener('pointerdown', onPointerDown)
    el.addEventListener('pointermove', onPointerMove)
    el.addEventListener('pointerup', onPointerUp)
    el.addEventListener('wheel', pauseAndDebounce)
    return () => {
      el.removeEventListener('pointerdown', onPointerDown)
      el.removeEventListener('pointermove', onPointerMove)
      el.removeEventListener('pointerup', onPointerUp)
      el.removeEventListener('wheel', pauseAndDebounce)
      clearTimeout(interactionTimerRef.current)
    }
  }, [autoRotateEnabled, isAnalysisActive, setAutoRotate])

  // Fly-to on selection
  const flyToEntity = useCallback((lat: number, lng: number) => {
    const globe = globeRef.current
    if (!globe) return
    globe.pointOfView({ lat, lng, altitude: 0.8 }, 1000)
  }, [])

  // ─── Data fetching effects ───────────────────────────────────────────

  // Fetch paths when source and target are set
  useEffect(() => {
    if (!pathModeEnabled || !pathSource || !pathTarget) return
    setPathLoading(true)
    setSelectedPathIndex(0)
    fetchISISPaths(pathSource, pathTarget, 5, pathMode)
      .then(result => {
        setPathsResult(result)
        if (result.paths?.length > 0) {
          if (overlays.deviceType) toggleOverlay('deviceType')
          if (overlays.linkType) toggleOverlay('linkType')
        }
      })
      .catch(err => setPathsResult({ paths: [], from: pathSource, to: pathTarget, error: err.message }))
      .finally(() => setPathLoading(false))
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pathModeEnabled, pathSource, pathTarget, pathMode])

  // Fetch reverse paths
  useEffect(() => {
    if (!pathModeEnabled || !pathSource || !pathTarget || !showReverse) {
      setReversePathsResult(null)
      return
    }
    setReversePathLoading(true)
    setSelectedReversePathIndex(0)
    fetchISISPaths(pathTarget, pathSource, 5, pathMode)
      .then(result => setReversePathsResult(result))
      .catch(err => setReversePathsResult({ paths: [], from: pathTarget, to: pathSource, error: err.message }))
      .finally(() => setReversePathLoading(false))
  }, [pathModeEnabled, pathSource, pathTarget, pathMode, showReverse])

  // Fetch metro paths
  useEffect(() => {
    if (!metroPathModeEnabled || !metroPathSource || !metroPathTarget) return
    setMetroPathLoading(true)
    setMetroPathViewMode('aggregate')
    setMetroPathSelectedPairs([])
    fetchMetroDevicePaths(metroPathSource, metroPathTarget, pathMode)
      .then(result => {
        setMetroPathsResult(result)
        if (result.devicePairs?.length > 0) {
          if (overlays.deviceType) toggleOverlay('deviceType')
          if (overlays.linkType) toggleOverlay('linkType')
        }
      })
      .catch(err => setMetroPathsResult({
        fromMetroPK: metroPathSource, fromMetroCode: '', toMetroPK: metroPathTarget, toMetroCode: '',
        sourceDeviceCount: 0, targetDeviceCount: 0, totalPairs: 0, minHops: 0, maxHops: 0,
        minLatencyMs: 0, maxLatencyMs: 0, avgLatencyMs: 0, devicePairs: [], error: err.message,
      }))
      .finally(() => setMetroPathLoading(false))
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [metroPathModeEnabled, metroPathSource, metroPathTarget, pathMode])

  // Clear state on mode exit
  useEffect(() => { if (!pathModeEnabled) { setPathSource(null); setPathTarget(null); setPathsResult(null); setSelectedPathIndex(0) } }, [pathModeEnabled])
  useEffect(() => { if (!metroPathModeEnabled) { setMetroPathSource(null); setMetroPathTarget(null); setMetroPathsResult(null); setMetroPathSelectedPairs([]); setMetroPathViewMode('aggregate') } }, [metroPathModeEnabled])
  useEffect(() => { if (!whatifRemovalMode) { setRemovalLink(null); setRemovalResult(null) } }, [whatifRemovalMode])
  useEffect(() => { if (!whatifAdditionMode) { setAdditionSource(null); setAdditionTarget(null); setAdditionResult(null) } }, [whatifAdditionMode])
  useEffect(() => { if (!impactMode) setImpactResult(null) }, [impactMode])
  useEffect(() => { if (!metroClusteringMode) setCollapsedMetros(new Set()) }, [metroClusteringMode])

  // Fetch link removal simulation
  useEffect(() => {
    if (!removalLink) return
    setRemovalLoading(true)
    fetchSimulateLinkRemoval(removalLink.sourcePK, removalLink.targetPK)
      .then(result => setRemovalResult(result))
      .catch(err => setRemovalResult({ sourcePK: removalLink.sourcePK, sourceCode: '', targetPK: removalLink.targetPK, targetCode: '', disconnectedDevices: [], disconnectedCount: 0, affectedPaths: [], affectedPathCount: 0, causesPartition: false, error: err.message }))
      .finally(() => setRemovalLoading(false))
  }, [removalLink])

  // Fetch link addition simulation
  useEffect(() => {
    if (!additionSource || !additionTarget) return
    setAdditionLoading(true)
    fetchSimulateLinkAddition(additionSource, additionTarget, additionMetric)
      .then(result => setAdditionResult(result))
      .catch(err => setAdditionResult({ sourcePK: additionSource, sourceCode: '', targetPK: additionTarget, targetCode: '', metric: additionMetric, improvedPaths: [], improvedPathCount: 0, redundancyGains: [], redundancyCount: 0, error: err.message }))
      .finally(() => setAdditionLoading(false))
  }, [additionSource, additionTarget, additionMetric])

  // Analyze failure impact
  useEffect(() => {
    if (impactDevices.length === 0) { setImpactResult(null); return }
    setImpactLoading(true)
    fetchWhatIfRemoval(impactDevices, [])
      .then(result => setImpactResult(result))
      .catch(err => setImpactResult({ items: [], totalAffectedPaths: 0, totalDisconnected: 0, error: err.message }))
      .finally(() => setImpactLoading(false))
  }, [impactDevices])

  // Pre-populate source when entering analysis modes
  const prevModeRef = useRef<string>(mode)
  useEffect(() => {
    const selectedDevicePK = selectedItem?.type === 'device' ? selectedItem.data.pk : null
    if (whatifAdditionMode && prevModeRef.current !== 'whatif-addition' && selectedDevicePK) setAdditionSource(selectedDevicePK)
    if (pathModeEnabled && prevModeRef.current !== 'path' && selectedDevicePK) setPathSource(selectedDevicePK)
    if (impactMode && prevModeRef.current !== 'impact' && selectedDevicePK && !impactDevices.includes(selectedDevicePK)) toggleImpactDevice(selectedDevicePK)
    prevModeRef.current = mode
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, whatifAdditionMode, pathModeEnabled, impactMode, selectedItem])

  // ─── URL param sync ──────────────────────────────────────────────────

  const [modeParamsRestored, setModeParamsRestored] = useState(false)

  useEffect(() => {
    if (!modeParamsRestored) return
    const params = new URLSearchParams(searchParams)
    let changed = false
    const setParam = (key: string, value: string | null) => {
      if (value) { if (params.get(key) !== value) { params.set(key, value); changed = true } }
      else if (params.has(key)) { params.delete(key); changed = true }
    }
    const inAnalysisMode = pathModeEnabled || whatifRemovalMode || whatifAdditionMode || impactMode
    if (inAnalysisMode) { setParam('type', null); setParam('id', null) }
    setParam('path_source', pathModeEnabled ? pathSource : null)
    setParam('path_target', pathModeEnabled ? pathTarget : null)
    setParam('removal_link', whatifRemovalMode ? removalLink?.linkPK ?? null : null)
    setParam('addition_source', whatifAdditionMode ? additionSource : null)
    setParam('addition_target', whatifAdditionMode ? additionTarget : null)
    setParam('impact_devices', impactMode && impactDevices.length > 0 ? impactDevices.join(',') : null)
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
    if (changed) setSearchParams(params, { replace: true })
  }, [modeParamsRestored, searchParams, setSearchParams, pathModeEnabled, pathSource, pathTarget, whatifRemovalMode, removalLink, whatifAdditionMode, additionSource, additionTarget, impactMode, impactDevices, multicastTreesMode, selectedMulticastGroup, multicastGroupDetails, enabledPublishers, enabledSubscribers])

  // Restore mode from URL on initial load
  useEffect(() => {
    if (modeParamsRestored) return
    if (deviceMap.size === 0 || linkMap.size === 0) return

    const pathSourceParam = searchParams.get('path_source')
    const pathTargetParam = searchParams.get('path_target')
    const removalLinkParam = searchParams.get('removal_link')
    const additionSourceParam = searchParams.get('addition_source')
    const additionTargetParam = searchParams.get('addition_target')
    const impactDevicesParam = searchParams.get('impact_devices')
    const multicastParam = searchParams.get('multicast')

    if (pathSourceParam || pathTargetParam) {
      if (pathSourceParam && !deviceMap.has(pathSourceParam)) return
      if (pathTargetParam && !deviceMap.has(pathTargetParam)) return
      if (pathSourceParam) setPathSource(pathSourceParam)
      if (pathTargetParam) setPathTarget(pathTargetParam)
      if (mode !== 'path') { setMode('path'); openPanel('mode') }
      setModeParamsRestored(true)
      return
    }
    if (removalLinkParam) {
      const link = linkMap.get(removalLinkParam)
      if (link) {
        setRemovalLink({ sourcePK: link.side_a_pk, targetPK: link.side_z_pk, linkPK: link.pk })
        if (mode !== 'whatif-removal') { setMode('whatif-removal'); openPanel('mode') }
        setModeParamsRestored(true)
        return
      }
      return
    }
    if (additionSourceParam || additionTargetParam) {
      if (additionSourceParam && !deviceMap.has(additionSourceParam)) return
      if (additionTargetParam && !deviceMap.has(additionTargetParam)) return
      if (additionSourceParam) setAdditionSource(additionSourceParam)
      if (additionTargetParam) setAdditionTarget(additionTargetParam)
      if (mode !== 'whatif-addition') { setMode('whatif-addition'); openPanel('mode') }
      setModeParamsRestored(true)
      return
    }
    if (impactDevicesParam) {
      const devicePKs = impactDevicesParam.split(',').filter(Boolean)
      if (!devicePKs.every(pk => deviceMap.has(pk))) return
      for (const pk of devicePKs) { if (!impactDevices.includes(pk)) toggleImpactDevice(pk) }
      if (mode !== 'impact') { setMode('impact'); openPanel('mode') }
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

    setModeParamsRestored(true)
  }, [modeParamsRestored, searchParams, deviceMap, linkMap, mode, setMode, openPanel, impactDevices, toggleImpactDevice, overlays.multicastTrees, toggleOverlay])

  // Restore selected item from URL params
  const lastProcessedParamsRef = useRef<string | null>(null)
  useEffect(() => {
    const type = searchParams.get('type')
    const id = searchParams.get('id')
    if (!type || !id) return
    const paramsKey = `${type}:${id}`
    if (lastProcessedParamsRef.current === paramsKey) return

    let itemFound = false

    if (type === 'validator') {
      const validator = validatorMap.get(id)
      if (validator) {
        const device = deviceMap.get(validator.device_pk)
        const metro = device ? metroMap.get(device.metro_pk) : undefined
        setSelectedItemState({
          type: 'validator',
          data: {
            votePubkey: validator.vote_pubkey, nodePubkey: validator.node_pubkey, tunnelId: validator.tunnel_id,
            city: validator.city || 'Unknown', country: validator.country || 'Unknown',
            stakeSol: (validator.stake_sol ?? 0) >= 1e6 ? `${(validator.stake_sol / 1e6).toFixed(2)}M` : (validator.stake_sol ?? 0) >= 1e3 ? `${(validator.stake_sol / 1e3).toFixed(0)}k` : `${(validator.stake_sol ?? 0).toFixed(0)}`,
            stakeShare: (validator.stake_share ?? 0) > 0 ? `${validator.stake_share.toFixed(2)}%` : '0%',
            commission: validator.commission ?? 0, version: validator.version || '',
            gossipIp: validator.gossip_ip || '', gossipPort: validator.gossip_port ?? 0,
            tpuQuicIp: validator.tpu_quic_ip || '', tpuQuicPort: validator.tpu_quic_port ?? 0,
            deviceCode: device?.code || 'Unknown', devicePk: validator.device_pk,
            metroPk: device?.metro_pk || '', metroName: metro?.name || 'Unknown',
            inRate: formatTrafficRate(validator.in_bps), outRate: formatTrafficRate(validator.out_bps),
          },
        })
        if (!showValidators) toggleOverlay('validators')
        if (metro && !pathModeEnabled && !impactMode) flyToEntity(metro.latitude, metro.longitude)
        itemFound = true
      }
    } else if (type === 'device') {
      const device = deviceMap.get(id)
      if (device) {
        const metro = metroMap.get(device.metro_pk)
        setSelectedItemState({
          type: 'device',
          data: {
            pk: device.pk, code: device.code, deviceType: device.device_type, status: device.status,
            metroPk: device.metro_pk, metroName: metro?.name || 'Unknown',
            contributorPk: device.contributor_pk, contributorCode: device.contributor_code,
            userCount: device.user_count ?? 0, validatorCount: device.validator_count ?? 0,
            stakeSol: device.stake_sol ?? 0, stakeShare: device.stake_share ?? 0,
            interfaces: device.interfaces || [],
          },
        })
        if (metro && !pathModeEnabled && !impactMode) flyToEntity(metro.latitude, metro.longitude)
        itemFound = true
        if (impactMode) { if (!impactDevices.includes(id)) toggleImpactDevice(id) }
        else if (pathModeEnabled) { if (!pathSource) setPathSource(id); else if (!pathTarget && id !== pathSource) setPathTarget(id) }
        else if (whatifAdditionMode) { if (!additionSource) setAdditionSource(id); else if (!additionTarget && id !== additionSource) setAdditionTarget(id) }
      }
    } else if (type === 'link') {
      const link = linkMap.get(id)
      if (link) {
        setSelectedItemState({ type: 'link', data: buildLinkInfo(link) })
        if (whatifRemovalMode) setRemovalLink({ sourcePK: link.side_a_pk, targetPK: link.side_z_pk, linkPK: link.pk })
        itemFound = true
      }
    } else if (type === 'metro') {
      const metro = metroMap.get(id)
      if (metro) {
        setSelectedItemState({ type: 'metro', data: { pk: metro.pk, code: metro.code, name: metro.name, deviceCount: devicesByMetro.get(metro.pk)?.length || 0 } })
        flyToEntity(metro.latitude, metro.longitude)
        itemFound = true
      }
    }

    if (itemFound && mode === 'explore') openPanel('details')
    if (itemFound) lastProcessedParamsRef.current = paramsKey
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams, validatorMap, deviceMap, linkMap, metroMap, devicesByMetro, showValidators, toggleOverlay, mode, openPanel, impactMode, pathModeEnabled, whatifAdditionMode, whatifRemovalMode, pathSource, pathTarget, additionSource, additionTarget])

  // ─── Keyboard shortcuts ──────────────────────────────────────────────

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return
      if (e.metaKey || e.ctrlKey || e.altKey) return
      const isInMode = mode !== 'explore'

      if (e.key === 'Escape') {
        if (mode !== 'explore') setMode('explore')
        else if (selectedItem) setSelectedItem(null)
      } else if (e.key === 'p' && !isInMode) setMode('path')
      else if (e.key === 'c' && !isInMode) toggleOverlay('criticality')
      else if (e.key === 'r' && !isInMode) setMode('whatif-removal')
      else if (e.key === 'a' && !isInMode) setMode('whatif-addition')
      else if (e.key === 's' && !isInMode) toggleOverlay('stake')
      else if (e.key === 'h' && !isInMode) toggleOverlay('linkHealth')
      else if (e.key === 't' && !isInMode) toggleOverlay('trafficFlow')
      else if (e.key === 'm' && !isInMode) toggleOverlay('metroClustering')
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [mode, setMode, toggleOverlay, selectedItem, setSelectedItem])

  // ─── Path finding handlers ───────────────────────────────────────────

  const handlePathDeviceClick = useCallback((devicePK: string) => {
    if (!pathSource) setPathSource(devicePK)
    else if (!pathTarget && devicePK !== pathSource) setPathTarget(devicePK)
    else { setPathSource(devicePK); setPathTarget(null); setPathsResult(null); setSelectedPathIndex(0) }
  }, [pathSource, pathTarget])

  const clearPath = useCallback(() => {
    setPathSource(null); setPathTarget(null); setPathsResult(null); setSelectedPathIndex(0)
    setShowReverse(false); setReversePathsResult(null); setSelectedReversePathIndex(0)
  }, [])

  const toggleMetroCollapse = useCallback((metroPK: string) => {
    setCollapsedMetros(prev => {
      const s = new Set(prev)
      if (s.has(metroPK)) s.delete(metroPK)
      else s.add(metroPK)
      return s
    })
  }, [])

  // Criticality colors
  const criticalityColors: Record<string, string> = {
    critical: '#ef4444',
    important: '#eab308',
    redundant: '#22c55e',
  }

  // ─── Build points data ───────────────────────────────────────────────

  const pointsDataRaw: GlobePointEntity[] = useMemo(() => {
    const pts: GlobePointEntity[] = []

    // Add device points
    for (const device of devices) {
      const pos = devicePositions.get(device.pk)
      if (!pos) continue
      // Hide device if metro is collapsed in clustering mode
      if (metroClusteringMode && collapsedMetros.has(device.metro_pk)) continue
      const metro = metroMap.get(device.metro_pk)
      pts.push({
        entityType: 'device',
        pk: device.pk, code: device.code, deviceType: device.device_type, status: device.status,
        lat: pos.lat, lng: pos.lng, metroPk: device.metro_pk,
        metroName: metro?.name || 'Unknown',
        contributorPk: device.contributor_pk, contributorCode: device.contributor_code,
        stakeSol: device.stake_sol ?? 0, stakeShare: device.stake_share ?? 0,
        userCount: device.user_count ?? 0, validatorCount: device.validator_count ?? 0,
        interfaces: device.interfaces || [],
      })
    }

    // Add metro points when metro clustering is active (for collapsed metro markers)
    if (metroClusteringMode) {
      for (const metro of metros) {
        pts.push({
          entityType: 'metro',
          pk: metro.pk, code: metro.code, name: metro.name,
          lat: metro.latitude, lng: metro.longitude,
          deviceCount: devicesByMetro.get(metro.pk)?.length || 0,
        })
      }
    }

    // Add validator points
    if (visibleValidators.length > 0) {
      for (const v of visibleValidators) {
        const device = deviceMap.get(v.device_pk)
        const metro = device ? metroMap.get(device.metro_pk) : undefined
        pts.push({
          entityType: 'validator',
          votePubkey: v.vote_pubkey, nodePubkey: v.node_pubkey, tunnelId: v.tunnel_id,
          devicePk: v.device_pk,
          lat: v.latitude, lng: v.longitude,
          stakeSol: v.stake_sol ?? 0, stakeShare: v.stake_share ?? 0,
          city: v.city || 'Unknown', country: v.country || 'Unknown',
          commission: v.commission ?? 0, version: v.version || '',
          gossipIp: v.gossip_ip || '', gossipPort: v.gossip_port ?? 0,
          tpuQuicIp: v.tpu_quic_ip || '', tpuQuicPort: v.tpu_quic_port ?? 0,
          deviceCode: device?.code || 'Unknown', metroPk: device?.metro_pk || '',
          metroName: metro?.name || 'Unknown',
          inBps: v.in_bps ?? 0, outBps: v.out_bps ?? 0,
        })
      }
    }

    return pts
  }, [devices, devicePositions, metroClusteringMode, collapsedMetros, metroMap, metros, devicesByMetro, visibleValidators, deviceMap])

  // Stabilize pointsData — return previous reference if content unchanged
  // to prevent react-globe.gl from re-animating on data refetch.
  const prevPointsRef = useRef<GlobePointEntity[]>([])
  const prevPointsKeyRef = useRef('')
  const stablePointsData = useMemo(() => {
    const key = pointsDataRaw.map(p => {
      if (p.entityType === 'device') return `d:${p.pk}`
      if (p.entityType === 'metro') return `m:${p.pk}`
      return `v:${(p as GlobePointValidator).votePubkey}`
    }).join(',')
    if (key === prevPointsKeyRef.current) return prevPointsRef.current
    prevPointsKeyRef.current = key
    prevPointsRef.current = pointsDataRaw
    return pointsDataRaw
  }, [pointsDataRaw])

  // ─── Build arcs data ─────────────────────────────────────────────────

  const arcsDataRaw: GlobeArcEntity[] = useMemo(() => {
    const arcs: GlobeArcEntity[] = []
    const interMetroEdges = new Map<string, { count: number; totalLatency: number; latencyCount: number }>()

    for (const link of links) {
      // Handle collapsed metros in clustering mode
      if (metroClusteringMode) {
        const deviceA = deviceMap.get(link.side_a_pk)
        const deviceZ = deviceMap.get(link.side_z_pk)
        const metroAPK = deviceA?.metro_pk
        const metroZPK = deviceZ?.metro_pk
        if (metroAPK && metroZPK) {
          const aCollapsed = collapsedMetros.has(metroAPK)
          const zCollapsed = collapsedMetros.has(metroZPK)
          // Skip intra-metro links when the metro is collapsed
          if (metroAPK === metroZPK && aCollapsed) continue
          // Aggregate inter-metro links when at least one end is collapsed
          if (aCollapsed || zCollapsed) {
            if (metroAPK !== metroZPK) {
              const edgeKey = [metroAPK, metroZPK].sort().join('|')
              const existing = interMetroEdges.get(edgeKey) || { count: 0, totalLatency: 0, latencyCount: 0 }
              existing.count++
              if (link.latency_us > 0) {
                existing.totalLatency += link.latency_us
                existing.latencyCount++
              }
              interMetroEdges.set(edgeKey, existing)
            }
            // Skip individual link if both ends collapsed
            if (aCollapsed && zCollapsed) continue
          }
        }
      }

      const startPos = devicePositions.get(link.side_a_pk)
      const endPos = devicePositions.get(link.side_z_pk)
      if (!startPos || !endPos) continue

      arcs.push({
        entityType: 'link',
        pk: link.pk, code: link.code, linkType: link.link_type,
        startLat: startPos.lat, startLng: startPos.lng,
        endLat: endPos.lat, endLng: endPos.lng,
        bandwidthBps: link.bandwidth_bps, latencyUs: link.latency_us,
        jitterUs: link.jitter_us ?? 0,
        latencyAtoZUs: link.latency_a_to_z_us, jitterAtoZUs: link.jitter_a_to_z_us ?? 0,
        latencyZtoAUs: link.latency_z_to_a_us, jitterZtoAUs: link.jitter_z_to_a_us ?? 0,
        lossPercent: link.loss_percent ?? 0,
        inBps: link.in_bps, outBps: link.out_bps,
        deviceAPk: link.side_a_pk, deviceACode: link.side_a_code || 'Unknown',
        interfaceAName: link.side_a_iface_name || '', interfaceAIP: link.side_a_ip || '',
        deviceZPk: link.side_z_pk, deviceZCode: link.side_z_code || 'Unknown',
        interfaceZName: link.side_z_iface_name || '', interfaceZIP: link.side_z_ip || '',
        contributorPk: link.contributor_pk, contributorCode: link.contributor_code,
        sampleCount: link.sample_count ?? 0, committedRttNs: link.committed_rtt_ns,
        isisDelayOverrideNs: link.isis_delay_override_ns,
      })
    }

    // Add aggregated inter-metro arcs for collapsed metros
    if (metroClusteringMode && collapsedMetros.size > 0) {
      for (const [edgeKey, data] of interMetroEdges) {
        const [metroAPK, metroZPK] = edgeKey.split('|')
        const metroA = metroMap.get(metroAPK)
        const metroZ = metroMap.get(metroZPK)
        if (!metroA || !metroZ) continue
        if (!collapsedMetros.has(metroAPK) && !collapsedMetros.has(metroZPK)) continue
        arcs.push({
          entityType: 'inter-metro',
          metroAPk: metroAPK, metroACode: metroA.code,
          metroZPk: metroZPK, metroZCode: metroZ.code,
          linkCount: data.count,
          avgLatencyUs: data.latencyCount > 0 ? data.totalLatency / data.latencyCount : 0,
          startLat: metroA.latitude, startLng: metroA.longitude,
          endLat: metroZ.latitude, endLng: metroZ.longitude,
        })
      }
    }

    // Add validator connecting lines
    if (visibleValidators.length > 0) {
      for (const v of visibleValidators) {
        const devicePos = devicePositions.get(v.device_pk)
        if (!devicePos) continue
        arcs.push({
          entityType: 'validator-link',
          votePubkey: v.vote_pubkey,
          devicePk: v.device_pk,
          role: multicastPublisherDevices.has(v.device_pk) ? 'pub' : multicastSubscriberDevices.has(v.device_pk) ? 'sub' : null,
          startLat: v.latitude, startLng: v.longitude,
          endLat: devicePos.lat, endLng: devicePos.lng,
        })
      }
    }

    // Add per-publisher multicast tree arcs (separate from regular link arcs)
    if (multicastTreesMode && selectedMulticastGroup && multicastPublisherPaths.length > 0) {
      for (const { publisherPK, segments } of multicastPublisherPaths) {
        const colorIndex = multicastPublisherColorMap.get(publisherPK) ?? 0
        for (const seg of segments) {
          const canonicalKey = [seg.fromPK, seg.toPK].sort().join('|')
          const pubs = multicastSegmentPublishers.get(canonicalKey) ?? [publisherPK]
          const pubIndex = pubs.indexOf(publisherPK)
          const altitude = getPublisherAltitude(pubIndex, pubs.length, seg.fromLat, seg.fromLng, seg.toLat, seg.toLng)

          arcs.push({
            entityType: 'multicast-tree',
            publisherPK,
            colorIndex,
            startLat: seg.fromLat, startLng: seg.fromLng,
            endLat: seg.toLat, endLng: seg.toLng,
            altitude,
          })
        }
      }
    }

    return arcs
  }, [links, devicePositions, metroClusteringMode, deviceMap, collapsedMetros, metroMap, visibleValidators, multicastTreesMode, selectedMulticastGroup, multicastPublisherPaths, multicastSegmentPublishers, multicastPublisherColorMap, multicastPublisherDevices, multicastSubscriberDevices])

  // Stabilize arcsData — return previous reference if content unchanged
  // to prevent react-globe.gl from re-animating on data refetch.
  const prevArcsRef = useRef<GlobeArcEntity[]>([])
  const prevArcsKeyRef = useRef('')
  const stableArcsData = useMemo(() => {
    const key = arcsDataRaw.map(a => {
      if (a.entityType === 'link') return `l:${a.pk}`
      if (a.entityType === 'inter-metro') return `im:${(a as GlobeArcInterMetro).metroAPk}-${(a as GlobeArcInterMetro).metroZPk}`
      if (a.entityType === 'multicast-tree') return `mt:${(a as GlobeArcMulticastTree).publisherPK}-${a.startLat}-${a.endLat}`
      return `vl:${(a as GlobeArcValidatorLink).votePubkey}`
    }).join(',')
    if (key === prevArcsKeyRef.current) return prevArcsRef.current
    prevArcsKeyRef.current = key
    prevArcsRef.current = arcsDataRaw
    return arcsDataRaw
  }, [arcsDataRaw])

  // ─── Point accessors ─────────────────────────────────────────────────

  const getPointColor = useCallback((point: object) => {
    const p = point as GlobePointEntity

    if (p.entityType === 'validator') {
      const v = p as GlobePointValidator
      const isSelected = selectedItem?.type === 'validator' && selectedItem.data.votePubkey === v.votePubkey
      if (isSelected) return '#3b82f6'
      const roleColor = multicastDeviceRoleColorMap.get(v.devicePk)
      return roleColor || '#a855f7'
    }

    if (p.entityType === 'metro') {
      const isSelected = selectedItem?.type === 'metro' && selectedItem.data.pk === p.pk
      if (isSelected) return '#3b82f6'
      if (metroClusteringMode) {
        const metroIndex = metroIndexMap.get(p.pk) ?? 0
        return METRO_COLORS[metroIndex % METRO_COLORS.length]
      }
      return '#00ccaa'
    }

    // Device entity
    const d = p as GlobePointDevice
    const isSelected = selectedItem?.type === 'device' && selectedItem.data.pk === d.pk
    const isPathSource = pathSource === d.pk
    const isPathTarget = pathTarget === d.pk
    const devicePathIndices = devicePathMap.get(d.pk)
    const isInSelectedPath = devicePathIndices?.includes(selectedPathIndex)
    const isInAnyPath = devicePathIndices && devicePathIndices.length > 0
    const metroDevicePathIndices = metroDevicePathMap.get(d.pk)
    const isInSelectedMetroPath = metroPathSelectedPairs.length > 0 && metroDevicePathIndices?.some(idx => metroPathSelectedPairs.includes(idx))
    const isInAnyMetroPath = metroDevicePathIndices && metroDevicePathIndices.length > 0
    const isInAnyMulticastTree = multicastTreeDevicePKs.has(d.pk)
    const isMulticastPublisher = multicastPublisherDevices.has(d.pk)
    const isMulticastSubscriber = multicastSubscriberDevices.has(d.pk)
    const isISISEnabled = isisDevicePKs.size === 0 || isisDevicePKs.has(d.pk)
    const isDisabledInPathMode = pathModeEnabled && !isISISEnabled
    const isAdditionSource = additionSource === d.pk
    const isAdditionTarget = additionTarget === d.pk
    const isDisconnected = disconnectedDevicePKs.has(d.pk)
    const isImpactDevice = impactMode && impactDevices.includes(d.pk)

    // Priority cascade (same as map)
    if (isDisabledInPathMode) return '#4b5563'
    if (whatifRemovalMode && isDisconnected) return '#ef4444'
    if (isAdditionSource || isPathSource) return '#22c55e'
    if (isAdditionTarget || isPathTarget) return '#ef4444'
    if (isInSelectedPath && devicePathIndices) return PATH_COLORS[selectedPathIndex % PATH_COLORS.length]
    if (isInAnyPath && devicePathIndices) return PATH_COLORS[devicePathIndices[0] % PATH_COLORS.length]
    if (metroPathModeEnabled && isInSelectedMetroPath) return PATH_COLORS[0]
    if (metroPathModeEnabled && isInAnyMetroPath && metroDevicePathIndices) return PATH_COLORS[metroDevicePathIndices[0] % PATH_COLORS.length]
    if (multicastTreesMode && isMulticastPublisher) { const mc = MULTICAST_PUBLISHER_COLORS[(multicastPublisherColorMap.get(d.pk) ?? 0) % MULTICAST_PUBLISHER_COLORS.length]; return isDark ? mc.dark : mc.light }
    if (multicastTreesMode && isMulticastSubscriber) return '#ef4444'
    if (multicastTreesMode && isInAnyMulticastTree) {
      // Find the first publisher that routes through this device
      const firstPub = multicastPublisherPaths.find(pp => pp.segments.some(s => s.fromPK === d.pk || s.toPK === d.pk))
      const firstPubPK = firstPub?.publisherPK
      const cIdx = firstPubPK ? (multicastPublisherColorMap.get(firstPubPK) ?? 0) : 0
      const mc = MULTICAST_PUBLISHER_COLORS[cIdx % MULTICAST_PUBLISHER_COLORS.length]
      return isDark ? mc.dark : mc.light
    }
    if (isImpactDevice) return '#ef4444'
    if (isSelected) return '#3b82f6'

    // Overlay colors
    if (stakeOverlayMode) return getStakeColor(d.stakeShare)
    if (contributorDevicesMode && !stakeOverlayMode && !metroClusteringMode) {
      if (!d.contributorPk) return '#6b7280'
      return CONTRIBUTOR_COLORS[(contributorIndexMap.get(d.contributorPk) ?? 0) % CONTRIBUTOR_COLORS.length]
    }
    if (metroClusteringMode && !stakeOverlayMode && !contributorDevicesMode) return METRO_COLORS[(metroIndexMap.get(d.metroPk) ?? 0) % METRO_COLORS.length]
    if (deviceTypeMode && !stakeOverlayMode && !metroClusteringMode && !contributorDevicesMode) return DEVICE_TYPE_COLORS[d.deviceType?.toLowerCase() || 'default'] || DEVICE_TYPE_COLORS.default

    // Vibrant default for the "living demo" aesthetic
    return '#00ffcc'
  }, [selectedItem, pathSource, pathTarget, devicePathMap, selectedPathIndex, metroDevicePathMap, metroPathSelectedPairs, multicastTreeDevicePKs, multicastPublisherPaths, multicastPublisherDevices, multicastSubscriberDevices, isisDevicePKs, pathModeEnabled, additionSource, additionTarget, disconnectedDevicePKs, impactMode, impactDevices, whatifRemovalMode, stakeOverlayMode, contributorDevicesMode, contributorIndexMap, metroClusteringMode, metroIndexMap, deviceTypeMode, metroPathModeEnabled, multicastTreesMode, multicastPublisherColorMap, multicastDeviceRoleColorMap])

  const getPointRadius = useCallback((point: object) => {
    const p = point as GlobePointEntity

    if (p.entityType === 'validator') return calculateValidatorGlobeRadius((p as GlobePointValidator).stakeSol)
    if (p.entityType === 'metro') {
      if (collapsedMetros.has(p.pk)) return 0.5  // Super metro marker
      return 0.35  // Normal metro marker
    }

    // Device
    const d = p as GlobePointDevice
    if (stakeOverlayMode) return calculateDeviceStakeGlobeRadius(d.stakeSol)

    // Larger for selected/highlighted states
    const isPathSource = pathSource === d.pk
    const isPathTarget = pathTarget === d.pk
    const isAdditionSource = additionSource === d.pk
    const isAdditionTarget = additionTarget === d.pk
    const isImpactDevice = impactMode && impactDevices.includes(d.pk)
    const isSelected = selectedItem?.type === 'device' && selectedItem.data.pk === d.pk

    if (isPathSource || isPathTarget || isAdditionSource || isAdditionTarget || isImpactDevice) return 0.25
    if (isSelected) return 0.2
    return 0.15
  }, [collapsedMetros, stakeOverlayMode, pathSource, pathTarget, additionSource, additionTarget, impactMode, impactDevices, selectedItem])

  const getPointAltitude = useCallback((point: object) => {
    const p = point as GlobePointEntity
    if (p.entityType === 'validator') return 0.005
    if (p.entityType === 'metro') return 0.015  // Above devices
    return 0.01
  }, [])

  const getPointLabel = useCallback((point: object) => {
    const p = point as GlobePointEntity

    if (p.entityType === 'validator') {
      const v = p as GlobePointValidator
      return `<div style="background:rgba(0,0,0,0.85);padding:6px 10px;border-radius:6px;font-size:12px;color:#fff;max-width:240px">
        <div style="color:#a855f7;font-size:10px">Validator</div>
        <div style="font-weight:600;font-family:monospace">${v.votePubkey.slice(0, 12)}...</div>
      </div>`
    }

    if (p.entityType === 'metro') {
      const m = p as GlobePointMetro
      return `<div style="background:rgba(0,0,0,0.85);padding:6px 10px;border-radius:6px;font-size:12px;color:#fff">
        <div style="color:#9ca3af;font-size:10px">Metro</div>
        <div style="font-weight:600">${m.code}</div>
        <div style="color:#9ca3af">${m.name} &middot; ${m.deviceCount} device${m.deviceCount !== 1 ? 's' : ''}</div>
      </div>`
    }

    const d = p as GlobePointDevice
    return `<div style="background:rgba(0,0,0,0.85);padding:6px 10px;border-radius:6px;font-size:12px;color:#fff">
      <div style="font-weight:600">${d.code}</div>
      <div style="color:#9ca3af">Type: <span style="color:#fff;text-transform:capitalize">${d.deviceType}</span></div>
      ${d.contributorCode ? `<div style="color:#9ca3af">Contributor: <span style="color:#fff">${d.contributorCode}</span></div>` : ''}
    </div>`
  }, [])

  // ─── Arc accessors ───────────────────────────────────────────────────

  const getArcColor = useCallback((arc: object) => {
    const a = arc as GlobeArcEntity

    if (a.entityType === 'multicast-tree') {
      const mt = a as GlobeArcMulticastTree
      const mc = MULTICAST_PUBLISHER_COLORS[mt.colorIndex % MULTICAST_PUBLISHER_COLORS.length]
      return isDark ? mc.dark : mc.light
    }
    if (a.entityType === 'validator-link') {
      const vl = a as GlobeArcValidatorLink
      const roleColor = multicastDeviceRoleColorMap.get(vl.devicePk)
      if (roleColor) return [roleColor, roleColor]
      return ['rgba(168,85,247,0.7)', 'rgba(124,58,237,0.9)']
    }
    if (a.entityType === 'inter-metro') {
      const m = a as GlobeArcInterMetro
      if (metroClusteringMode) {
        const colorA = METRO_COLORS[(metroIndexMap.get(m.metroAPk) ?? 0) % METRO_COLORS.length]
        const colorZ = METRO_COLORS[(metroIndexMap.get(m.metroZPk) ?? 0) % METRO_COLORS.length]
        return [colorA, colorZ]
      }
      return ['rgba(0,255,204,0.6)', 'rgba(59,130,246,0.6)']
    }

    const l = a as GlobeArcLink
    const isSelected = selectedItem?.type === 'link' && selectedItem.data.pk === l.pk
    const linkPathIndices = linkPathMap.get(l.pk)
    const isInSelectedPath = linkPathIndices?.includes(selectedPathIndex)
    const isInAnyPath = linkPathIndices && linkPathIndices.length > 0
    const metroLinkPathIndices = metroLinkPathMap.get(l.pk)
    const isInSelectedMetroPath = metroPathSelectedPairs.length > 0 && metroLinkPathIndices?.some(idx => metroPathSelectedPairs.includes(idx))
    const isInAnyMetroPath = metroLinkPathIndices && metroLinkPathIndices.length > 0
    const criticality = linkCriticalityMap.get(l.pk)
    const isRemovedLink = removalLink?.linkPK === l.pk

    // Priority cascade
    if (whatifRemovalMode && isRemovedLink) return '#ef4444'
    if (linkHealthMode) {
      const sla = linkSlaStatus.get(l.pk)
      if (sla) {
        if (sla.status === 'healthy') return '#22c55e'
        if (sla.status === 'warning') return '#eab308'
        if (sla.status === 'critical') return '#ef4444'
      }
      return '#6b728080'
    }
    if (trafficFlowMode) return getTrafficColor(linkMap.get(l.pk)!).color
    if (contributorLinksMode && l.contributorPk) return CONTRIBUTOR_COLORS[(contributorIndexMap.get(l.contributorPk) ?? 0) % CONTRIBUTOR_COLORS.length]
    if (contributorLinksMode && !l.contributorPk) return '#6b728050'
    if (linkTypeMode) {
      const linkType = l.linkType || 'default'
      const colors = LINK_TYPE_COLORS[linkType] || LINK_TYPE_COLORS.default
      return colors.dark
    }
    if (criticalityOverlayEnabled && criticality) return criticalityColors[criticality]
    if (isisHealthMode) {
      const healthKey = `${l.deviceAPk}|${l.deviceZPk}`
      const healthStatus = edgeHealthStatus.get(healthKey)
      if (healthStatus === 'missing') return '#ef4444'
      if (healthStatus === 'extra') return '#f59e0b'
      if (healthStatus === 'mismatch') return '#eab308'
      return '#22c55e'
    }
    if (isInSelectedPath && linkPathIndices) return PATH_COLORS[selectedPathIndex % PATH_COLORS.length]
    if (isInAnyPath && linkPathIndices) return PATH_COLORS[linkPathIndices[0] % PATH_COLORS.length] + '80'
    if (metroPathModeEnabled && isInSelectedMetroPath) return PATH_COLORS[0]
    if (metroPathModeEnabled && isInAnyMetroPath && metroLinkPathIndices) return PATH_COLORS[metroLinkPathIndices[0] % PATH_COLORS.length] + '60'
    if (isSelected) return '#3b82f6'

    // Dim all regular link arcs when multicast overlay is active and dimming enabled
    // (multicast-tree arcs handle per-publisher rendering separately)
    if (multicastTreesMode && dimOtherLinks) return 'rgba(100,100,100,0.08)'

    // Vibrant default gradient for the "living demo" aesthetic
    return ['rgba(0,255,204,0.6)', 'rgba(59,130,246,0.6)']
  }, [selectedItem, linkPathMap, selectedPathIndex, metroLinkPathMap, metroPathSelectedPairs, linkCriticalityMap, removalLink, whatifRemovalMode, linkHealthMode, linkSlaStatus, trafficFlowMode, linkMap, contributorLinksMode, contributorIndexMap, linkTypeMode, criticalityOverlayEnabled, criticalityColors, isisHealthMode, edgeHealthStatus, metroPathModeEnabled, multicastTreesMode, metroClusteringMode, metroIndexMap, dimOtherLinks, isDark, multicastDeviceRoleColorMap])

  const getArcStroke = useCallback((arc: object) => {
    const a = arc as GlobeArcEntity
    if (a.entityType === 'multicast-tree') return 0.6
    if (a.entityType === 'validator-link') return 0.15
    if (a.entityType === 'inter-metro') return 0.8

    const l = a as GlobeArcLink
    const isSelected = selectedItem?.type === 'link' && selectedItem.data.pk === l.pk
    const isRemovedLink = removalLink?.linkPK === l.pk
    const linkPathIndices = linkPathMap.get(l.pk)
    const isInSelectedPath = linkPathIndices?.includes(selectedPathIndex)
    const metroLinkPathIndices = metroLinkPathMap.get(l.pk)
    const isInSelectedMetroPath = metroPathSelectedPairs.length > 0 && metroLinkPathIndices?.some(idx => metroPathSelectedPairs.includes(idx))

    // Dim regular link arcs when multicast overlay is active
    if (multicastTreesMode && dimOtherLinks) return 0

    if (bandwidthMode) return getBandwidthStroke(l.bandwidthBps)
    if (isRemovedLink || isInSelectedPath || isInSelectedMetroPath) return 0.8
    if (isSelected) return 0.7
    if (trafficFlowMode) return getTrafficColor(linkMap.get(l.pk)!).stroke
    return bandwidthMode ? getBandwidthStroke(l.bandwidthBps) : 0.35
  }, [selectedItem, removalLink, linkPathMap, selectedPathIndex, metroLinkPathMap, metroPathSelectedPairs, bandwidthMode, trafficFlowMode, linkMap, multicastTreesMode, dimOtherLinks])

  const getArcDashLength = useCallback((arc: object) => {
    const a = arc as GlobeArcEntity
    // Multicast-tree arcs always dash when animate flow is on
    if (a.entityType === 'multicast-tree') return animateFlow ? 0.4 : 0
    // Validator-link arcs: dash when multicast animate flow is on
    if (a.entityType === 'validator-link') return animateFlow ? 0.3 : 0
    // When animation is off, show solid lines (except mode-specific dashing)
    if (!linkAnimating) {
      if (a.entityType === 'link') {
        const l = a as GlobeArcLink
        const isRemovedLink = removalLink?.linkPK === l.pk
        const criticality = linkCriticalityMap.get(l.pk)
        if (whatifRemovalMode && isRemovedLink) return 0.3
        if (criticalityOverlayEnabled && criticality) return 0.3
      }
      return 0
    }
    if (a.entityType === 'inter-metro') return 0
    return 0.4
  }, [removalLink, whatifRemovalMode, criticalityOverlayEnabled, linkCriticalityMap, linkAnimating, animateFlow])

  const getArcDashGap = useCallback((arc: object) => {
    const a = arc as GlobeArcEntity
    if (a.entityType === 'multicast-tree') return animateFlow ? 0.2 : 0
    if (a.entityType === 'validator-link') return animateFlow ? 0.2 : 0
    if (!linkAnimating) {
      if (a.entityType === 'link') {
        const l = a as GlobeArcLink
        const isRemovedLink = removalLink?.linkPK === l.pk
        const criticality = linkCriticalityMap.get(l.pk)
        if (whatifRemovalMode && isRemovedLink) return 0.2
        if (criticalityOverlayEnabled && criticality) return 0.2
      }
      return 0
    }
    if (a.entityType === 'inter-metro') return 0
    return 0.2
  }, [removalLink, whatifRemovalMode, criticalityOverlayEnabled, linkCriticalityMap, linkAnimating, animateFlow])

  const getArcLabel = useCallback((arc: object) => {
    const a = arc as GlobeArcEntity
    if (a.entityType === 'multicast-tree') return '' // No tooltip for multicast tree arcs
    if (a.entityType === 'validator-link') return ''
    // Skip tooltip on dimmed links when multicast overlay is active
    if (multicastTreesMode && dimOtherLinks && a.entityType === 'link') return ''
    if (a.entityType === 'inter-metro') {
      const m = a as GlobeArcInterMetro
      const avgLatencyMs = m.avgLatencyUs > 0 ? (m.avgLatencyUs / 1000).toFixed(2) + ' ms' : 'N/A'
      return `<div style="background:rgba(0,0,0,0.85);padding:6px 10px;border-radius:6px;font-size:12px;color:#fff">
        <div style="font-weight:600">${m.metroACode} ↔ ${m.metroZCode}</div>
        <div style="color:#9ca3af">${m.linkCount} link${m.linkCount !== 1 ? 's' : ''} &middot; Avg latency: <span style="color:#fff">${avgLatencyMs}</span></div>
      </div>`
    }

    const l = a as GlobeArcLink
    const latencyMs = (l.latencyUs / 1000).toFixed(2)
    return `<div style="background:rgba(0,0,0,0.85);padding:6px 10px;border-radius:6px;font-size:12px;color:#fff;max-width:280px">
      <div style="font-weight:600">${l.code}</div>
      <div style="color:#9ca3af">A: <span style="color:#fff">${l.deviceACode}</span>${l.interfaceAName ? ` <span style="font-family:monospace;color:#fff">(${l.interfaceAName})</span>` : ''}</div>
      <div style="color:#9ca3af">Z: <span style="color:#fff">${l.deviceZCode}</span>${l.interfaceZName ? ` <span style="font-family:monospace;color:#fff">(${l.interfaceZName})</span>` : ''}</div>
      <div style="color:#9ca3af">Latency: <span style="color:#fff">${l.latencyUs > 0 ? latencyMs + ' ms' : 'N/A'}</span> &middot; BW: <span style="color:#fff">${formatBandwidth(l.bandwidthBps)}</span></div>
    </div>`
  }, [multicastTreesMode, dimOtherLinks])

  // ─── Click handlers ──────────────────────────────────────────────────

  const handlePointClick = useCallback((point: object) => {
    const p = point as GlobePointEntity

    if (p.entityType === 'validator') {
      const v = p as GlobePointValidator
      const item: SelectedItemData = {
        type: 'validator',
        data: {
          votePubkey: v.votePubkey, nodePubkey: v.nodePubkey, tunnelId: v.tunnelId,
          city: v.city, country: v.country,
          stakeSol: v.stakeSol >= 1e6 ? `${(v.stakeSol / 1e6).toFixed(2)}M` : v.stakeSol >= 1e3 ? `${(v.stakeSol / 1e3).toFixed(0)}k` : `${v.stakeSol.toFixed(0)}`,
          stakeShare: v.stakeShare > 0 ? `${v.stakeShare.toFixed(2)}%` : '0%',
          commission: v.commission, version: v.version,
          gossipIp: v.gossipIp, gossipPort: v.gossipPort,
          tpuQuicIp: v.tpuQuicIp, tpuQuicPort: v.tpuQuicPort,
          deviceCode: v.deviceCode, devicePk: v.devicePk,
          metroPk: v.metroPk, metroName: v.metroName,
          inRate: formatTrafficRate(v.inBps), outRate: formatTrafficRate(v.outBps),
        },
      }
      setSelectedItem(item)
      flyToEntity(v.lat, v.lng)
      return
    }

    if (p.entityType === 'metro') {
      const m = p as GlobePointMetro
      if (metroClusteringMode && collapsedMetros.has(m.pk)) {
        toggleMetroCollapse(m.pk)
        return
      }
      const item: SelectedItemData = {
        type: 'metro',
        data: { pk: m.pk, code: m.code, name: m.name, deviceCount: m.deviceCount },
      }
      setSelectedItem(item)
      flyToEntity(m.lat, m.lng)
      return
    }

    // Device click
    const d = p as GlobePointDevice

    if (pathModeEnabled) {
      const isISISEnabled = isisDevicePKs.size === 0 || isisDevicePKs.has(d.pk)
      if (isISISEnabled) handlePathDeviceClick(d.pk)
      return
    }

    if (whatifAdditionMode) {
      if (!additionSource) setAdditionSource(d.pk)
      else if (!additionTarget && d.pk !== additionSource) setAdditionTarget(d.pk)
      else { setAdditionSource(d.pk); setAdditionTarget(null); setAdditionResult(null) }
      return
    }

    if (impactMode) {
      toggleImpactDevice(d.pk)
      return
    }

    const item: SelectedItemData = {
      type: 'device',
      data: {
        pk: d.pk, code: d.code, deviceType: d.deviceType, status: d.status,
        metroPk: d.metroPk, metroName: d.metroName,
        contributorPk: d.contributorPk, contributorCode: d.contributorCode,
        userCount: d.userCount, validatorCount: d.validatorCount,
        stakeSol: d.stakeSol, stakeShare: d.stakeShare,
        interfaces: d.interfaces,
      },
    }
    setSelectedItem(item)
    flyToEntity(d.lat, d.lng)
  }, [setSelectedItem, flyToEntity, pathModeEnabled, isisDevicePKs, handlePathDeviceClick, whatifAdditionMode, additionSource, additionTarget, impactMode, toggleImpactDevice, metroClusteringMode, collapsedMetros, toggleMetroCollapse])

  const handleArcClick = useCallback((arc: object) => {
    const a = arc as GlobeArcEntity
    if (a.entityType === 'multicast-tree') return
    if (a.entityType === 'validator-link') return
    if (a.entityType === 'inter-metro') return
    if (pathModeEnabled || whatifAdditionMode) return

    const l = a as GlobeArcLink
    const link = linkMap.get(l.pk)
    if (!link) return

    if (whatifRemovalMode) {
      setRemovalLink({ sourcePK: link.side_a_pk, targetPK: link.side_z_pk, linkPK: link.pk })
      return
    }

    const item: SelectedItemData = { type: 'link', data: buildLinkInfo(link) }
    setSelectedItem(item)
  }, [linkMap, buildLinkInfo, setSelectedItem, pathModeEnabled, whatifAdditionMode, whatifRemovalMode])

  const handleGlobeClick = useCallback(() => {
    setSelectedItem(null)
  }, [setSelectedItem])

  // ─── Selection ring ──────────────────────────────────────────────────

  const ringsData = useMemo(() => {
    if (!selectedItem) return []
    if (selectedItem.type === 'device') {
      const pos = devicePositions.get(selectedItem.data.pk)
      if (pos) return [{ lat: pos.lat, lng: pos.lng }]
    } else if (selectedItem.type === 'metro') {
      const metro = metroMap.get(selectedItem.data.pk)
      if (metro) return [{ lat: metro.latitude, lng: metro.longitude }]
    } else if (selectedItem.type === 'validator') {
      const v = validatorMap.get(selectedItem.data.votePubkey)
      if (v) return [{ lat: v.latitude, lng: v.longitude }]
    }
    return []
  }, [selectedItem, devicePositions, metroMap, validatorMap])

  // ─── Render ──────────────────────────────────────────────────────────

  return (
    <div ref={containerRef} className="absolute inset-0 bg-black">
      {(!globeReady || themeSwitching) && (
        <div className="absolute inset-0 z-10 flex items-center justify-center bg-black">
          <div className="h-8 w-8 animate-spin rounded-full border-2 border-white/40 border-t-white" />
        </div>
      )}
      {texturesLoaded && dimensions.width > 0 && dimensions.height > 0 && (
        <Globe
          ref={globeRefCb}
          width={dimensions.width}
          height={dimensions.height}
          globeImageUrl={globeImageUrl}
          backgroundImageUrl="/textures/night-sky.jpg"
          showAtmosphere={true}
          atmosphereColor={isDark ? '#1a73e8' : '#6baadb'}
          atmosphereAltitude={isDark ? 0.2 : 0.15}
          animateIn={true}
          lineHoverPrecision={1}
          // Points layer
          pointsData={stablePointsData}
          pointLat="lat"
          pointLng="lng"
          pointRadius={getPointRadius}
          pointColor={getPointColor}
          pointLabel={getPointLabel}
          pointAltitude={getPointAltitude}
          pointResolution={12}
          onPointClick={handlePointClick}
          // Arcs layer
          arcsData={stableArcsData}
          arcStartLat="startLat"
          arcStartLng="startLng"
          arcEndLat="endLat"
          arcEndLng="endLng"
          arcColor={getArcColor}
          arcStroke={getArcStroke}
          arcDashLength={getArcDashLength}
          arcDashGap={getArcDashGap}
          arcDashAnimateTime={(d: object) => {
            const a = d as GlobeArcEntity
            // Multicast-tree arcs: always animate in publisher→subscriber direction
            if (a.entityType === 'multicast-tree') return animateFlow ? 3000 : 0
            if (a.entityType === 'validator-link') {
              if (!animateFlow) return 0
              const vl = a as GlobeArcValidatorLink
              // Positive = start→end (validator→device), negative = reverse
              return vl.role === 'sub' ? -4000 : 4000
            }
            if (!linkAnimating) return 0
            if (a.entityType === 'inter-metro') return 0
            return arcAnimateTime((a as GlobeArcLink).latencyUs)
          }}
          arcAltitude={(d: object) => {
            const a = d as GlobeArcEntity
            if (a.entityType === 'multicast-tree') return (a as GlobeArcMulticastTree).altitude
            return undefined as unknown as number // Let auto-scale handle it
          }}
          arcAltitudeAutoScale={0.3}
          arcLabel={getArcLabel}
          onArcClick={handleArcClick}
          // Selection ring
          ringsData={ringsData}
          ringColor={() => '#3b82f6'}
          ringMaxRadius={3}
          ringPropagationSpeed={2}
          ringRepeatPeriod={1000}
          // Globe click (deselect)
          onGlobeClick={handleGlobeClick}
        />
      )}

      {/* Control bar */}
      <TopologyControlBar
        autoRotating={autoRotating}
        onToggleAutoRotate={() => {
          const next = !autoRotating
          setAutoRotateEnabled(next)
          setAutoRotate(next)
        }}
        linkAnimating={linkAnimating}
        onToggleLinkAnimation={() => setLinkAnimating(prev => !prev)}
      />

      {/* Detail panel - shown when entity selected in explore mode */}
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
                <TopologyEntityLink to={`/dz/devices/${(selectedItem.data as LinkInfo).deviceAPk}`}>{(selectedItem.data as LinkInfo).deviceACode}</TopologyEntityLink>
                {' ↔ '}
                <TopologyEntityLink to={`/dz/devices/${(selectedItem.data as LinkInfo).deviceZPk}`}>{(selectedItem.data as LinkInfo).deviceZCode}</TopologyEntityLink>
              </span>
            ) : undefined
          }
        >
          {/* eslint-disable @typescript-eslint/no-explicit-any */}
          {selectedItem.type === 'device' && <DeviceDetails device={selectedItem.data as any} />}
          {selectedItem.type === 'link' && <LinkDetails link={selectedItem.data as any} />}
          {selectedItem.type === 'metro' && <MetroDetails metro={selectedItem.data as any} />}
          {selectedItem.type === 'validator' && <ValidatorDetails validator={selectedItem.data as any} />}
          {/* eslint-enable @typescript-eslint/no-explicit-any */}
        </TopologyPanel>
      )}

      {/* Mode panel - shown when in analysis mode */}
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
              pathSource={pathSource} pathTarget={pathTarget} pathsResult={pathsResult}
              pathLoading={pathLoading} pathMode={pathMode} selectedPathIndex={selectedPathIndex}
              devices={deviceOptions} showReverse={showReverse} reversePathsResult={reversePathsResult}
              reversePathLoading={reversePathLoading} selectedReversePathIndex={selectedReversePathIndex}
              onPathModeChange={setPathMode} onSelectPath={setSelectedPathIndex}
              onSelectReversePath={setSelectedReversePathIndex} onClearPath={clearPath}
              onSetSource={setPathSource} onSetTarget={setPathTarget}
              onToggleReverse={() => setShowReverse(prev => !prev)}
            />
          )}
          {mode === 'metro-path' && (
            <MetroPathModePanel
              sourceMetro={metroPathSource} targetMetro={metroPathTarget} metros={metroOptions}
              pathsResult={metroPathsResult} loading={metroPathLoading} pathMode={pathMode}
              viewMode={metroPathViewMode} selectedPairIndices={metroPathSelectedPairs}
              onSetSourceMetro={setMetroPathSource} onSetTargetMetro={setMetroPathTarget}
              onPathModeChange={setPathMode} onViewModeChange={setMetroPathViewMode}
              onTogglePair={handleToggleMetroPathPair}
              onClearSelection={() => setMetroPathSelectedPairs([])}
              onClear={() => { setMetroPathSource(null); setMetroPathTarget(null); setMetroPathsResult(null); setMetroPathSelectedPairs([]); setMetroPathViewMode('aggregate') }}
            />
          )}
          {mode === 'whatif-removal' && (
            <WhatIfRemovalPanel
              removalLink={removalLink} result={removalResult} isLoading={removalLoading}
              onClear={() => { setRemovalLink(null); setRemovalResult(null) }}
            />
          )}
          {mode === 'whatif-addition' && (
            <WhatIfAdditionPanel
              additionSource={additionSource} additionTarget={additionTarget}
              additionMetric={additionMetric} result={additionResult} isLoading={additionLoading}
              onMetricChange={setAdditionMetric}
              onClear={() => { setAdditionSource(null); setAdditionTarget(null); setAdditionResult(null) }}
            />
          )}
          {mode === 'impact' && (
            <ImpactPanel
              devicePKs={impactDevices} deviceCodes={deviceCodeMap}
              result={impactResult} isLoading={impactLoading}
              onRemoveDevice={toggleImpactDevice}
              onClear={() => { clearImpactDevices(); setImpactResult(null) }}
            />
          )}
        </TopologyPanel>
      )}

      {/* Overlay panel - shown when overlay is active */}
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
          {isisHealthMode && <ComparePanel data={compareData ?? null} isLoading={compareLoading} />}
          {linkHealthMode && <LinkHealthOverlayPanel linkHealthData={linkHealthData} isLoading={!linkHealthData} />}
          {trafficFlowMode && <TrafficFlowOverlayPanel edgeTrafficMap={edgeTrafficMap} links={links} isLoading={links.length === 0} />}
          {criticalityOverlayEnabled && <CriticalityPanel data={criticalLinksData ?? null} isLoading={!criticalLinksData} />}
          {metroClusteringMode && (
            <MetroClusteringOverlayPanel
              metroInfoMap={metroInfoMap}
              collapsedMetros={collapsedMetros}
              getMetroColor={(pk) => METRO_COLORS[(metroIndexMap.get(pk) ?? 0) % METRO_COLORS.length]}
              getDeviceCountForMetro={(pk) => devicesByMetro.get(pk)?.length ?? 0}
              totalDeviceCount={devices.length}
              onToggleMetroCollapse={toggleMetroCollapse}
              onCollapseAll={() => setCollapsedMetros(new Set(metroInfoMap.keys()))}
              onExpandAll={() => setCollapsedMetros(new Set())}
              isLoading={metros.length === 0}
            />
          )}
          {showValidators && <ValidatorsOverlayPanel validators={validators} isLoading={validators.length === 0} />}
          {(contributorDevicesMode || contributorLinksMode) && (
            <ContributorsOverlayPanel
              contributorInfoMap={contributorInfoMap}
              getContributorColor={(pk) => CONTRIBUTOR_COLORS[(contributorIndexMap.get(pk) ?? 0) % CONTRIBUTOR_COLORS.length]}
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
              onSetAllPublishers={handleSetAllPublishers}
              onSetAllSubscribers={handleSetAllSubscribers}
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
    </div>
  )
}
