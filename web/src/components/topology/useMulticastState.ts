import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { fetchMulticastGroup, fetchMulticastTreeSegments } from '@/lib/api'
import type { MulticastGroupDetail, MulticastTreeSegmentsResponse } from '@/lib/api'
import { MULTICAST_PUBLISHER_COLORS } from './overlays/MulticastTreesOverlayPanel'

export interface UseMulticastStateOptions {
  enabled: boolean
  isDark: boolean
}

export interface MulticastParamState {
  groupCode: string | null
  disabledPubs: Set<string> | null
  disabledSubs: Set<string> | null
}

// Cache key: sorted publisher device PKs joined by comma
type SegmentCacheKey = string

export function useMulticastState({ enabled, isDark }: UseMulticastStateOptions) {
  const [selectedMulticastGroup, setSelectedMulticastGroup] = useState<string | null>(null)
  const [multicastGroupDetails, setMulticastGroupDetails] = useState<Map<string, MulticastGroupDetail>>(new Map())
  const [segmentCache, setSegmentCache] = useState<Map<string, Map<SegmentCacheKey, MulticastTreeSegmentsResponse>>>(new Map())
  const [enabledPublishers, setEnabledPublishers] = useState<Set<string>>(new Set())
  const [enabledSubscribers, setEnabledSubscribers] = useState<Set<string>>(new Set())
  const [dimOtherLinks, setDimOtherLinks] = useState(true)
  const [animateFlow, setAnimateFlow] = useState(true)
  const [showTreeValidators, setShowTreeValidators] = useState(true)
  const [combineSegments, setCombineSegments] = useState(true)
  const [hoveredMemberDevicePK, setHoveredMemberDevicePK] = useState<string | null>(null)

  // PKs to skip during auto-enable (restored from URL on initial load)
  const initialDisabledPubsRef = useRef<Set<string> | null>(null)
  const initialDisabledSubsRef = useRef<Set<string> | null>(null)
  // Track which group we've already initialized publishers/subscribers for
  const initializedGroupRef = useRef<string | null>(null)
  // Track in-flight fetch to avoid duplicates
  const fetchingSegmentsRef = useRef<string | null>(null)

  // Handler to select multicast group
  const handleSelectMulticastGroup = useCallback((code: string | null) => {
    setSelectedMulticastGroup(code)
    if (code !== selectedMulticastGroup) {
      initializedGroupRef.current = null
    }
  }, [selectedMulticastGroup])

  // Unified setters for panel's solo/cmd/shift click model
  const handleSetEnabledPublishers = useCallback((updater: Set<string> | ((prev: Set<string>) => Set<string>)) => {
    if (typeof updater === 'function') setEnabledPublishers(updater)
    else setEnabledPublishers(updater)
  }, [])

  const handleSetEnabledSubscribers = useCallback((updater: Set<string> | ((prev: Set<string>) => Set<string>)) => {
    if (typeof updater === 'function') setEnabledSubscribers(updater)
    else setEnabledSubscribers(updater)
  }, [])

  // Derive device-level enabled sets from user_pk-keyed enabled sets
  const enabledPublisherDevicePKs = useMemo(() => {
    const set = new Set<string>()
    if (!selectedMulticastGroup) return set
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return set
    for (const m of detail.members) {
      if ((m.mode === 'P' || m.mode === 'P+S') && enabledPublishers.has(m.user_pk)) {
        set.add(m.device_pk)
      }
    }
    return set
  }, [selectedMulticastGroup, multicastGroupDetails, enabledPublishers])

  const enabledSubscriberDevicePKs = useMemo(() => {
    const set = new Set<string>()
    if (!selectedMulticastGroup) return set
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return set
    for (const m of detail.members) {
      if ((m.mode === 'S' || m.mode === 'P+S') && enabledSubscribers.has(m.user_pk)) {
        set.add(m.device_pk)
      }
    }
    return set
  }, [selectedMulticastGroup, multicastGroupDetails, enabledSubscribers])

  // Stable cache key for the current set of enabled publisher device PKs
  const segmentCacheKey = useMemo(() => {
    return Array.from(enabledPublisherDevicePKs).sort().join(',')
  }, [enabledPublisherDevicePKs])

  // Current segments response from cache (if available)
  const currentSegmentsResponse = useMemo((): MulticastTreeSegmentsResponse | undefined => {
    if (!selectedMulticastGroup || !segmentCacheKey) return undefined
    return segmentCache.get(selectedMulticastGroup)?.get(segmentCacheKey)
  }, [selectedMulticastGroup, segmentCacheKey, segmentCache])

  // Fetch segments when enabled publishers change (debounced)
  useEffect(() => {
    if (!enabled || !selectedMulticastGroup) return
    if (enabledPublisherDevicePKs.size === 0) return

    const code = selectedMulticastGroup
    const cacheKey = segmentCacheKey

    // Already cached?
    if (segmentCache.get(code)?.has(cacheKey)) return

    // Already fetching this exact set?
    const fetchKey = `${code}:${cacheKey}`
    if (fetchingSegmentsRef.current === fetchKey) return

    // Debounce: wait 50ms before fetching to batch rapid toggling
    const timer = setTimeout(() => {
      fetchingSegmentsRef.current = fetchKey
      const publisherPKs = Array.from(enabledPublisherDevicePKs)

      fetchMulticastTreeSegments(code, publisherPKs)
        .then(result => {
          setSegmentCache(prev => {
            const next = new Map(prev)
            const groupCache = next.get(code) ?? new Map<SegmentCacheKey, MulticastTreeSegmentsResponse>()
            const updated = new Map(groupCache)
            updated.set(cacheKey, result)
            next.set(code, updated)
            return next
          })
        })
        .catch(err => console.error(`Failed to fetch multicast tree segments for ${code}:`, err))
        .finally(() => {
          if (fetchingSegmentsRef.current === fetchKey) {
            fetchingSegmentsRef.current = null
          }
        })
    }, 50)

    return () => clearTimeout(timer)
  }, [enabled, selectedMulticastGroup, enabledPublisherDevicePKs, segmentCacheKey, segmentCache])

  // Auto-load group details when group is selected, and refresh periodically
  useEffect(() => {
    if (!enabled || !selectedMulticastGroup) return
    const code = selectedMulticastGroup
    const load = () => {
      fetchMulticastGroup(code)
        .then(detail => setMulticastGroupDetails(prev => new Map(prev).set(code, detail)))
        .catch(err => console.error('Failed to fetch multicast group:', err))
    }
    if (!multicastGroupDetails.has(code)) load()
    const interval = setInterval(load, 30000)
    return () => clearInterval(interval)
  }, [enabled, selectedMulticastGroup]) // eslint-disable-line react-hooks/exhaustive-deps

  // Set enabled publishers/subscribers when group details are first loaded.
  // Only runs once per group selection — subsequent refreshes preserve user's selections.
  // Default: enable only the first publisher (+ all subscribers).
  useEffect(() => {
    if (!enabled || !selectedMulticastGroup) return
    if (initializedGroupRef.current === selectedMulticastGroup) return
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return

    initializedGroupRef.current = selectedMulticastGroup

    // On first load, skip PKs that were disabled in the URL
    const skipPubs = initialDisabledPubsRef.current
    const skipSubs = initialDisabledSubsRef.current
    initialDisabledPubsRef.current = null
    initialDisabledSubsRef.current = null

    // If restoring from URL, use the URL state (enable all except disabled)
    if (skipPubs || skipSubs) {
      const pubs = new Set<string>()
      const subs = new Set<string>()
      detail.members.forEach(m => {
        if ((m.mode === 'P' || m.mode === 'P+S') && !skipPubs?.has(m.user_pk)) {
          pubs.add(m.user_pk)
        }
        if ((m.mode === 'S' || m.mode === 'P+S') && !skipSubs?.has(m.user_pk)) {
          subs.add(m.user_pk)
        }
      })
      setEnabledPublishers(pubs)
      setEnabledSubscribers(subs)
      return
    }

    // Default: enable only first publisher + all subscribers
    const pubs = new Set<string>()
    const subs = new Set<string>()
    let firstPubAdded = false
    detail.members.forEach(m => {
      if ((m.mode === 'P' || m.mode === 'P+S') && !firstPubAdded) {
        pubs.add(m.user_pk)
        firstPubAdded = true
      }
      if (m.mode === 'S' || m.mode === 'P+S') {
        subs.add(m.user_pk)
      }
    })
    setEnabledPublishers(pubs)
    setEnabledSubscribers(subs)
  }, [enabled, selectedMulticastGroup, multicastGroupDetails])

  // Build publisher color map for consistent colors (shared with panel)
  const multicastPublisherColorMap = useMemo(() => {
    const map = new Map<string, number>()
    if (!enabled || !selectedMulticastGroup) return map

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
  }, [enabled, selectedMulticastGroup, multicastGroupDetails])

  // Aggregate segments across all publishers: one entry per unique (fromPK, toPK) pair.
  // Each segment carries the set of publishers that use it and a weight for rendering.
  // Now derived directly from the server response instead of client-side dedup.
  const multicastAggregatedSegments = useMemo(() => {
    const result: Array<{
      fromPK: string
      toPK: string
      publisherPKs: string[]
      publisherColorIndices: number[]
      weight: number
    }> = []
    if (!enabled || !selectedMulticastGroup || !currentSegmentsResponse) return result

    for (const seg of currentSegmentsResponse.segments) {
      result.push({
        fromPK: seg.fromPK,
        toPK: seg.toPK,
        publisherPKs: seg.publisherPKs,
        publisherColorIndices: seg.publisherPKs.map(pk => multicastPublisherColorMap.get(pk) ?? 0),
        weight: Math.min(2 + seg.publisherPKs.length * 1.5, 8),
      })
    }

    return result
  }, [enabled, selectedMulticastGroup, currentSegmentsResponse, multicastPublisherColorMap])

  // Per-publisher segment paths — only computed when combineSegments is off.
  // Each entry is one publisher's list of segments (fromPK, toPK pairs).
  // Derived from server segments by filtering each segment's publisherPKs.
  const multicastPublisherPaths = useMemo(() => {
    const result = new Map<string, Array<{ fromPK: string; toPK: string }>>()
    if (combineSegments || !enabled || !selectedMulticastGroup || !currentSegmentsResponse) return result

    for (const seg of currentSegmentsResponse.segments) {
      for (const pubPK of seg.publisherPKs) {
        if (!enabledPublisherDevicePKs.has(pubPK)) continue
        if (!result.has(pubPK)) result.set(pubPK, [])
        result.get(pubPK)!.push({ fromPK: seg.fromPK, toPK: seg.toPK })
      }
    }
    return result
  }, [combineSegments, enabled, selectedMulticastGroup, currentSegmentsResponse, enabledPublisherDevicePKs])

  // Canonical segment key -> list of publisher PKs that use this segment.
  // Used for per-publisher offset calculation when combineSegments is off.
  const multicastSegmentPublishers = useMemo(() => {
    const result = new Map<string, string[]>()
    if (combineSegments) return result
    for (const [pubPK, segments] of multicastPublisherPaths) {
      for (const seg of segments) {
        const key = [seg.fromPK, seg.toPK].sort().join('|')
        if (!result.has(key)) result.set(key, [])
        const pubs = result.get(key)!
        if (!pubs.includes(pubPK)) pubs.push(pubPK)
      }
    }
    return result
  }, [combineSegments, multicastPublisherPaths])

  // Set of device PKs that appear in any segment (for dimming non-tree elements)
  const multicastTreeDevicePKs = useMemo(() => {
    const set = new Set<string>()
    if (combineSegments) {
      for (const seg of multicastAggregatedSegments) {
        set.add(seg.fromPK)
        set.add(seg.toPK)
      }
    } else {
      for (const segments of multicastPublisherPaths.values()) {
        for (const seg of segments) {
          set.add(seg.fromPK)
          set.add(seg.toPK)
        }
      }
    }
    return set
  }, [combineSegments, multicastAggregatedSegments, multicastPublisherPaths])

  // When a member is hovered, determine which publisher device PKs should be highlighted
  const hoveredHighlightPublisherPKs = useMemo(() => {
    if (!hoveredMemberDevicePK || !enabled || !selectedMulticastGroup) return null
    if (enabledPublisherDevicePKs.has(hoveredMemberDevicePK)) {
      return new Set([hoveredMemberDevicePK])
    }
    // For a hovered subscriber, find publishers whose segments reach it
    if (enabledSubscriberDevicePKs.has(hoveredMemberDevicePK) && currentSegmentsResponse) {
      const pubs = new Set<string>()
      for (const seg of currentSegmentsResponse.segments) {
        if (seg.fromPK === hoveredMemberDevicePK || seg.toPK === hoveredMemberDevicePK) {
          for (const pubPK of seg.publisherPKs) {
            if (enabledPublisherDevicePKs.has(pubPK)) {
              pubs.add(pubPK)
            }
          }
        }
      }
      return pubs.size > 0 ? pubs : null
    }
    return null
  }, [hoveredMemberDevicePK, enabled, selectedMulticastGroup, enabledPublisherDevicePKs, enabledSubscriberDevicePKs, currentSegmentsResponse])

  // Map device_pk -> role color for validators on multicast member devices
  const multicastDeviceRoleColorMap = useMemo(() => {
    const map = new Map<string, string>()
    if (!enabled || !selectedMulticastGroup) return map
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return map
    for (const m of detail.members) {
      const isPub = (m.mode === 'P' || m.mode === 'P+S') && enabledPublishers.has(m.user_pk)
      const isSub = (m.mode === 'S' || m.mode === 'P+S') && enabledSubscribers.has(m.user_pk)
      if (isPub) {
        const colorIndex = multicastPublisherColorMap.get(m.device_pk) ?? 0
        const c = MULTICAST_PUBLISHER_COLORS[colorIndex % MULTICAST_PUBLISHER_COLORS.length]
        map.set(m.device_pk, isDark ? c.dark : c.light)
      } else if (isSub) {
        map.set(m.device_pk, '#14b8a6') // teal for subscriber
      }
    }
    return map
  }, [enabled, selectedMulticastGroup, multicastGroupDetails, multicastPublisherColorMap, enabledPublishers, enabledSubscribers, isDark])

  // Restore multicast state from URL params
  const restoreFromParams = useCallback((params: MulticastParamState) => {
    if (params.groupCode) {
      setSelectedMulticastGroup(params.groupCode)
      initializedGroupRef.current = null
    }
    if (params.disabledPubs) {
      initialDisabledPubsRef.current = params.disabledPubs
    }
    if (params.disabledSubs) {
      initialDisabledSubsRef.current = params.disabledSubs
    }
  }, [])

  // Get disabled params for URL persistence
  const getDisabledParams = useCallback(() => {
    if (!selectedMulticastGroup) return { disabledPubs: null as string[] | null, disabledSubs: null as string[] | null }
    const detail = multicastGroupDetails.get(selectedMulticastGroup)
    if (!detail?.members) return { disabledPubs: null as string[] | null, disabledSubs: null as string[] | null }
    const disabledPubs = detail.members
      .filter(m => (m.mode === 'P' || m.mode === 'P+S') && !enabledPublishers.has(m.user_pk))
      .map(m => m.user_pk)
    const disabledSubs = detail.members
      .filter(m => (m.mode === 'S' || m.mode === 'P+S') && !enabledSubscribers.has(m.user_pk))
      .map(m => m.user_pk)
    return {
      disabledPubs: disabledPubs.length > 0 ? disabledPubs : null,
      disabledSubs: disabledSubs.length > 0 ? disabledSubs : null,
    }
  }, [selectedMulticastGroup, multicastGroupDetails, enabledPublishers, enabledSubscribers])

  return {
    // State
    selectedMulticastGroup,
    multicastGroupDetails,
    enabledPublishers,
    enabledSubscribers,
    dimOtherLinks,
    setDimOtherLinks,
    animateFlow,
    setAnimateFlow,
    showTreeValidators,
    setShowTreeValidators,
    combineSegments,
    setCombineSegments,
    hoveredMemberDevicePK,
    setHoveredMemberDevicePK,

    // Derived
    enabledPublisherDevicePKs,
    enabledSubscriberDevicePKs,
    hoveredHighlightPublisherPKs,
    multicastPublisherColorMap,
    multicastDeviceRoleColorMap,
    multicastAggregatedSegments,
    multicastPublisherPaths,
    multicastSegmentPublishers,
    multicastTreeDevicePKs,

    // Handlers
    handleSelectMulticastGroup,
    handleSetEnabledPublishers,
    handleSetEnabledSubscribers,

    // URL param helpers
    restoreFromParams,
    getDisabledParams,
  }
}
