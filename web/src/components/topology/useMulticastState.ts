import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { fetchMulticastGroup, fetchMulticastTreePaths } from '@/lib/api'
import type { MulticastGroupDetail, MulticastTreeResponse, MulticastTreePath } from '@/lib/api'
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

// Per-publisher cache: groupCode -> (publisherDevicePK -> paths[])
type PublisherPathCache = Map<string, Map<string, MulticastTreePath[]>>

export function useMulticastState({ enabled, isDark }: UseMulticastStateOptions) {
  const [selectedMulticastGroup, setSelectedMulticastGroup] = useState<string | null>(null)
  const [multicastGroupDetails, setMulticastGroupDetails] = useState<Map<string, MulticastGroupDetail>>(new Map())
  const [publisherPathCache, setPublisherPathCache] = useState<PublisherPathCache>(new Map())
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
  // Track in-flight fetch requests to avoid duplicates
  const fetchingPublishersRef = useRef<Set<string>>(new Set())

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

  // Lazy fetch tree paths per publisher when enabled publishers change
  useEffect(() => {
    if (!enabled || !selectedMulticastGroup) return
    if (enabledPublisherDevicePKs.size === 0) return

    const code = selectedMulticastGroup
    const groupCache = publisherPathCache.get(code)

    // Find publishers that are enabled but not yet cached and not in-flight
    const uncachedPKs: string[] = []
    for (const pk of enabledPublisherDevicePKs) {
      if (!groupCache?.has(pk) && !fetchingPublishersRef.current.has(`${code}:${pk}`)) {
        uncachedPKs.push(pk)
      }
    }
    if (uncachedPKs.length === 0) return

    // Batch up to 5 publishers per request
    const batchSize = 5
    for (let i = 0; i < uncachedPKs.length; i += batchSize) {
      const batch = uncachedPKs.slice(i, i + batchSize)
      // Mark as in-flight
      for (const pk of batch) {
        fetchingPublishersRef.current.add(`${code}:${pk}`)
      }

      fetchMulticastTreePaths(code, batch)
        .then(result => {
          setPublisherPathCache(prev => {
            const next = new Map(prev)
            const existing = next.get(code) ?? new Map<string, MulticastTreePath[]>()
            const updated = new Map(existing)
            // Group returned paths by publisher device PK
            for (const pk of batch) {
              updated.set(pk, [])
            }
            if (result.paths) {
              for (const path of result.paths) {
                const pubPaths = updated.get(path.publisherDevicePK) ?? []
                pubPaths.push(path)
                updated.set(path.publisherDevicePK, pubPaths)
              }
            }
            next.set(code, updated)
            return next
          })
        })
        .catch(err => console.error(`Failed to fetch multicast tree paths for ${code} publishers ${batch.join(',')}:`, err))
        .finally(() => {
          for (const pk of batch) {
            fetchingPublishersRef.current.delete(`${code}:${pk}`)
          }
        })
    }
  }, [enabled, selectedMulticastGroup, enabledPublisherDevicePKs, publisherPathCache])

  // Assemble multicastTreePaths from the per-publisher cache for backward compatibility
  // This produces the same shape the views consume (Map<groupCode, MulticastTreeResponse>)
  const multicastTreePaths = useMemo(() => {
    const map = new Map<string, MulticastTreeResponse>()
    if (!selectedMulticastGroup) return map
    const groupCache = publisherPathCache.get(selectedMulticastGroup)
    if (!groupCache) return map

    const allPaths: MulticastTreePath[] = []
    for (const paths of groupCache.values()) {
      allPaths.push(...paths)
    }
    map.set(selectedMulticastGroup, {
      groupCode: selectedMulticastGroup,
      groupPK: '',
      publisherCount: groupCache.size,
      subscriberCount: 0,
      paths: allPaths,
    })
    return map
  }, [selectedMulticastGroup, publisherPathCache])

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
  const multicastAggregatedSegments = useMemo(() => {
    const result: Array<{
      fromPK: string
      toPK: string
      publisherPKs: string[]
      publisherColorIndices: number[]
      weight: number
    }> = []
    if (!enabled || !selectedMulticastGroup) return result

    const treeData = multicastTreePaths.get(selectedMulticastGroup)
    if (!treeData?.paths?.length) return result

    // Collect unique segments with their publisher sets
    // Use canonical key (sorted) to merge both directions
    const segmentMap = new Map<string, { fromPK: string; toPK: string; publishers: Set<string> }>()

    for (const treePath of treeData.paths) {
      const path = treePath.path
      if (!path?.length) continue
      if (!enabledPublisherDevicePKs.has(treePath.publisherDevicePK)) continue
      if (!enabledSubscriberDevicePKs.has(treePath.subscriberDevicePK)) continue

      for (let i = 0; i < path.length - 1; i++) {
        const fromPK = path[i].devicePK
        const toPK = path[i + 1].devicePK
        const canonicalKey = [fromPK, toPK].sort().join('|')

        let entry = segmentMap.get(canonicalKey)
        if (!entry) {
          entry = { fromPK, toPK, publishers: new Set() }
          segmentMap.set(canonicalKey, entry)
        }
        entry.publishers.add(treePath.publisherDevicePK)
      }
    }

    for (const entry of segmentMap.values()) {
      const publisherPKs = Array.from(entry.publishers)
      result.push({
        fromPK: entry.fromPK,
        toPK: entry.toPK,
        publisherPKs,
        publisherColorIndices: publisherPKs.map(pk => multicastPublisherColorMap.get(pk) ?? 0),
        weight: Math.min(2 + publisherPKs.length * 1.5, 8),
      })
    }

    return result
  }, [enabled, selectedMulticastGroup, multicastTreePaths, enabledPublisherDevicePKs, enabledSubscriberDevicePKs, multicastPublisherColorMap])

  // Per-publisher segment paths — only computed when combineSegments is off.
  // Each entry is one publisher's list of segments (fromPK, toPK pairs).
  const multicastPublisherPaths = useMemo(() => {
    const result = new Map<string, Array<{ fromPK: string; toPK: string }>>()
    if (combineSegments || !enabled || !selectedMulticastGroup) return result

    const treeData = multicastTreePaths.get(selectedMulticastGroup)
    if (!treeData?.paths?.length) return result

    for (const treePath of treeData.paths) {
      const path = treePath.path
      if (!path?.length) continue
      if (!enabledPublisherDevicePKs.has(treePath.publisherDevicePK)) continue
      if (!enabledSubscriberDevicePKs.has(treePath.subscriberDevicePK)) continue

      const pubPK = treePath.publisherDevicePK
      if (!result.has(pubPK)) result.set(pubPK, [])
      const segments = result.get(pubPK)!

      for (let i = 0; i < path.length - 1; i++) {
        const fromPK = path[i].devicePK
        const toPK = path[i + 1].devicePK
        // Deduplicate within this publisher
        const canonicalKey = [fromPK, toPK].sort().join('|')
        if (!segments.some(s => [s.fromPK, s.toPK].sort().join('|') === canonicalKey)) {
          segments.push({ fromPK, toPK })
        }
      }
    }
    return result
  }, [combineSegments, enabled, selectedMulticastGroup, multicastTreePaths, enabledPublisherDevicePKs, enabledSubscriberDevicePKs])

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
    if (enabledSubscriberDevicePKs.has(hoveredMemberDevicePK)) {
      const pubs = new Set<string>()
      const treeData = multicastTreePaths.get(selectedMulticastGroup)
      if (treeData?.paths) {
        for (const treePath of treeData.paths) {
          if (treePath.subscriberDevicePK === hoveredMemberDevicePK &&
              enabledPublisherDevicePKs.has(treePath.publisherDevicePK)) {
            pubs.add(treePath.publisherDevicePK)
          }
        }
      }
      return pubs.size > 0 ? pubs : null
    }
    return null
  }, [hoveredMemberDevicePK, enabled, selectedMulticastGroup, enabledPublisherDevicePKs, enabledSubscriberDevicePKs, multicastTreePaths])

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
    multicastTreePaths,
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
