import { useMemo, useCallback, useState, useEffect, type ReactNode } from 'react'
import MapGL, { Source, Layer, Marker } from 'react-map-gl/maplibre'
import type { MapLayerMouseEvent } from 'react-map-gl/maplibre'
import type { StyleSpecification } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from '@/hooks/use-theme'
import { usePlanner } from './PlannerContext'
import { buildDevicePositions, buildLinkFeatures, buildRemoveDeviceSnapshot } from './map-geojson'
import { deviceChangeStyle, CHANGE_LEGEND } from './change-styles'
import { LinkContextPopup } from './LinkContextPopup'
import { LinkEditForm } from './LinkEditForm'
import { nearestKeyWithin, SNAP_RADIUS_DEG } from './geo'
import { resolveMapClick, computeRubberBand } from './map-interactions'
import { MoveLinkEndForm } from './MoveLinkEndForm'
import { AddLinkForm } from './AddLinkForm'
import { AddDeviceForm, type AddDeviceSubmitValue } from './AddDeviceForm'
import { attachedLinks } from './attached-links'
import { estimateLatencyNs } from './estimator'

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
        attribution:
          '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      },
    },
    layers: [{ id: 'carto-tiles', type: 'raster', source: 'carto' }],
  }
}

export function PlannerMap() {
  const { draft, baseline, tool, setTool, selectedLinkKey, selectLink, addChange } =
    usePlanner()
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const [popupMode, setPopupMode] = useState<'menu' | 'edit'>('menu')
  const [dragSnapKey, setDragSnapKey] = useState<string | null>(null)
  const [pendingMove, setPendingMove] = useState<{
    side: 'a' | 'z'
    targetKey: string
  } | null>(null)
  const [addLinkSource, setAddLinkSource] = useState<string | null>(null)
  const [cursor, setCursor] = useState<[number, number] | null>(null)
  const [addLinkTarget, setAddLinkTarget] = useState<string | null>(null)
  const [placeDeviceAt, setPlaceDeviceAt] = useState<[number, number] | null>(null)
  // Always reopen the popup in its menu state when the selected link changes, so a
  // half-finished edit on one link never shows up over the next. Also drop any
  // in-progress endpoint drag / pending move: a move started on one link must never
  // survive a selection change and get confirmed against a different link (stale
  // move-state bug, same family as the edit-form reset above).
  useEffect(() => {
    setPopupMode('menu')
    setDragSnapKey(null)
    setPendingMove(null)
  }, [selectedLinkKey])
  // Reset any in-progress add-link rubber-band pick, and any pending add-device
  // placement, whenever the active tool changes -- leaving the tool for another one,
  // or re-entering it fresh. Without this, a source device (or a drop point) picked
  // in one session would survive a switch to another tool and back: the next
  // add-link click would be silently treated as the TARGET of the stale source, and
  // re-entering the add-device tool would reopen a form still pinned to the
  // abandoned drop point (same stale-state-across-selection family as the reset
  // above).
  useEffect(() => {
    setAddLinkSource(null)
    setAddLinkTarget(null)
    setCursor(null)
    setPlaceDeviceAt(null)
  }, [tool])
  const mapStyle = useMemo(() => createMapStyle(isDark), [isDark])

  const positions = useMemo(
    () => (draft ? buildDevicePositions(draft) : new Map<string, [number, number]>()),
    [draft]
  )

  const linkGeoJson = useMemo(
    () =>
      draft
        ? buildLinkFeatures(draft, positions, isDark, selectedLinkKey)
        : { type: 'FeatureCollection' as const, features: [] },
    [draft, positions, isDark, selectedLinkKey]
  )

  const handleMapClick = useCallback(
    (e: MapLayerMouseEvent) => {
      const feature = e.features?.[0]
      const action = resolveMapClick({
        tool,
        addLinkSource,
        addLinkTarget,
        lng: e.lngLat.lng,
        lat: e.lngLat.lat,
        positions,
        linkFeaturePk: feature?.properties?.pk ? String(feature.properties.pk) : null,
      })
      switch (action.kind) {
        case 'place-device':
          setPlaceDeviceAt([e.lngLat.lng, e.lngLat.lat])
          break
        case 'add-link-pick':
          if (!addLinkSource) setAddLinkSource(action.deviceKey)
          else setAddLinkTarget(action.deviceKey)
          break
        case 'select-link':
          selectLink(action.linkPk)
          break
        case 'deselect-link':
          selectLink(null)
          break
        case 'ignore':
          break
      }
    },
    [tool, addLinkSource, addLinkTarget, positions, selectLink]
  )

  // Device removal is the only tool still keyed off a specific device pk (rather
  // than a geo-snapped map click) -- it needs the exact device the operator clicked,
  // not the nearest one to some coordinate.
  const handleDeviceClick = useCallback(
    async (deviceKey: string) => {
      if (!draft || tool !== 'remove-device') return
      const dev = draft.deviceByKey.get(deviceKey)
      if (!dev) return
      // Stage a remove (or, once the operator adjusts it in the Changes panel, a
      // move) for each link still attached in the draft before removing the
      // device itself -- never a silent cascade. attachedLinks reads the live
      // draft, so a link already staged for removal is never staged twice.
      //
      // Await each staging call in sequence so the backend assigns each change a
      // strictly increasing seq (it derives seq via an unlocked SELECT MAX(seq)+10
      // per transaction). Firing these concurrently could give the remove_device a
      // seq below its own attached-link removals -- reproducing the impact engine's
      // "device still has attached link(s)" error -- or collide on the unique
      // (plan_id, seq) constraint and 500, silently dropping a change. Stage the
      // attached-link changes FIRST, the remove_device LAST. If any call fails,
      // stop rather than leave a half-staged partial teardown.
      try {
        for (const link of attachedLinks(draft, deviceKey)) {
          await addChange({
            op_type: 'remove_link',
            ref_link_pk: link.pk,
            payload: {},
            ref_snapshot: { link_code: link.code },
          })
        }
        await addChange({
          op_type: 'remove_device',
          ref_device_pk: dev.pk,
          payload: {},
          ref_snapshot: buildRemoveDeviceSnapshot(draft, dev),
        })
      } catch (err) {
        console.error('remove-device staging failed', err)
      }
    },
    [draft, tool, addChange]
  )

  const selectedLink =
    (selectedLinkKey && draft?.linkByKey.get(selectedLinkKey)) || null
  const selectedMidpoint = useMemo(() => {
    if (!selectedLink) return null
    const a = positions.get(selectedLink.side_a_pk)
    const b = positions.get(selectedLink.side_z_pk)
    if (!a || !b) return null
    return [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2] as [number, number]
  }, [selectedLink, positions])

  const handleDeleteLink = useCallback(() => {
    if (!selectedLink) return
    addChange({
      op_type: 'remove_link',
      ref_link_pk: selectedLink.pk,
      payload: {},
      ref_snapshot: { link_code: selectedLink.code },
    })
    selectLink(null)
  }, [selectedLink, addChange, selectLink])

  const handleEditLink = useCallback(
    (latencyNs: number, bandwidthBps: number) => {
      if (!selectedLink) return
      // In-place latency/bandwidth edit modeled as a move keeping the same endpoint.
      addChange({
        op_type: 'move_link_end',
        ref_link_pk: selectedLink.pk,
        new_device_pk: selectedLink.side_z_pk,
        payload: {
          side: 'z',
          new_iface_name: selectedLink.side_z_iface_name,
          latency_ns: latencyNs,
          bandwidth_bps: bandwidthBps,
          estimate_source: 'manual',
        },
        ref_snapshot: { link_code: selectedLink.code },
      })
      setPopupMode('menu')
      selectLink(null)
    },
    [selectedLink, addChange, selectLink]
  )

  const handleEndpointDragEnd = useCallback(
    (side: 'a' | 'z', lng: number, lat: number) => {
      if (!selectedLink) return
      // Exclude BOTH of this link's current endpoints: snapping to the dragged
      // side's own device is a no-op, and snapping to the OTHER endpoint's device
      // would collapse the link into a self-loop.
      const candidates = new Map(positions)
      candidates.delete(selectedLink.side_a_pk)
      candidates.delete(selectedLink.side_z_pk)
      const snapped = nearestKeyWithin(lng, lat, candidates, SNAP_RADIUS_DEG)
      setDragSnapKey(null)
      if (snapped) setPendingMove({ side, targetKey: snapped })
    },
    [selectedLink, positions]
  )

  const submitMove = useCallback(
    (latencyNs: number, bandwidthBps: number) => {
      if (!selectedLink || !pendingMove) return
      const target = draft?.deviceByKey.get(pendingMove.targetKey)
      addChange({
        op_type: 'move_link_end',
        ref_link_pk: selectedLink.pk,
        new_device_pk: target?.localRef ? null : pendingMove.targetKey,
        payload: {
          side: pendingMove.side,
          new_device_ref: target?.localRef ?? undefined,
          // The real interface is TBD -- the contributor decides it later.
          new_iface_name: 'TBD',
          latency_ns: latencyNs,
          bandwidth_bps: bandwidthBps,
          estimate_source: 'manual',
        },
        ref_snapshot: {
          link_code: selectedLink.code,
          device_code: target?.code,
        },
      })
      setPendingMove(null)
      selectLink(null)
    },
    [selectedLink, pendingMove, draft, addChange, selectLink]
  )

  const rubberBand = useMemo(
    () => computeRubberBand({ tool, addLinkSource, addLinkTarget, cursor, positions }),
    [tool, addLinkSource, addLinkTarget, cursor, positions]
  )

  const metroOfDevice = useCallback(
    (deviceKey: string) => draft?.deviceByKey.get(deviceKey)?.metro_pk ?? '',
    [draft]
  )

  const addLinkSuggestion = useMemo(() => {
    if (!baseline || !addLinkSource || !addLinkTarget) {
      return { latencyNs: 0, source: 'manual' as const }
    }
    return estimateLatencyNs({
      topology: baseline,
      metroAPk: metroOfDevice(addLinkSource),
      metroBPk: metroOfDevice(addLinkTarget),
    })
  }, [baseline, addLinkSource, addLinkTarget, metroOfDevice])

  const resetAddLink = useCallback(() => {
    setAddLinkSource(null)
    setAddLinkTarget(null)
    setCursor(null)
  }, [])

  const submitAddLink = useCallback(
    (v: {
      latencyNs: number
      bandwidthBps: number
      estimateSource: 'copied' | 'great_circle' | 'manual'
      linkType: 'WAN' | 'DZX'
    }) => {
      if (!addLinkSource || !addLinkTarget || !draft) return
      const a = draft.deviceByKey.get(addLinkSource)
      const z = draft.deviceByKey.get(addLinkTarget)
      addChange({
        op_type: 'add_link',
        local_ref: `tmp_link_${crypto.randomUUID().slice(0, 8)}`,
        payload: {
          side_a_device_pk: a?.localRef ? undefined : addLinkSource,
          side_a_ref: a?.localRef ?? undefined,
          side_z_device_pk: z?.localRef ? undefined : addLinkTarget,
          side_z_ref: z?.localRef ?? undefined,
          // The real interfaces are TBD -- the contributor decides them later.
          side_a_iface_name: 'TBD',
          side_z_iface_name: 'TBD',
          latency_ns: v.latencyNs,
          bandwidth_bps: v.bandwidthBps,
          estimate_source: v.estimateSource,
          link_type: v.linkType,
        },
        ref_snapshot: {
          link_code: `${a?.code ?? '?'}-${z?.code ?? '?'}`,
          side_a_contributor_code: a?.contributor_code,
          side_z_contributor_code: z?.contributor_code,
        },
      })
      resetAddLink()
      setTool('select')
    },
    [addLinkSource, addLinkTarget, draft, addChange, resetAddLink, setTool]
  )

  const metroPositions = useMemo(() => {
    const m = new Map<string, [number, number]>()
    for (const metro of draft?.metros ?? []) m.set(metro.pk, [metro.longitude, metro.latitude])
    return m
  }, [draft])

  const nearestMetroPk = useMemo(() => {
    if (!placeDeviceAt) return ''
    return nearestKeyWithin(placeDeviceAt[0], placeDeviceAt[1], metroPositions, 8) ?? ''
  }, [placeDeviceAt, metroPositions])

  const submitAddDevice = useCallback(
    (v: AddDeviceSubmitValue) => {
      const localRef = `tmp_dev_${crypto.randomUUID().slice(0, 8)}`
      // ref_snapshot carries human-readable identity (code, not pk) so it stays
      // meaningful after the pk is gone -- same invariant buildRemoveDeviceSnapshot
      // enforces for remove_device. Exactly one of metro_pk / new_metro is set,
      // matching the canonical add_device payload shape.
      const metroCode = v.metroPk
        ? (draft?.metros.find((m) => m.pk === v.metroPk)?.code ?? v.metroPk)
        : (v.newMetro?.code ?? '')
      addChange({
        op_type: 'add_device',
        local_ref: localRef,
        payload: {
          local_ref: localRef,
          code: v.code,
          contributor_code: v.contributorCode,
          contributor_pk: v.contributorPk,
          metro_pk: v.metroPk,
          new_metro: v.newMetro,
        },
        ref_snapshot: {
          device_code: v.code,
          metro_code: metroCode,
          contributor_code: v.contributorCode,
        },
      })
      setPlaceDeviceAt(null)
      setTool('select')
    },
    [draft, addChange, setTool]
  )

  // The single floating panel shown over the map, chosen from whichever
  // interactive form is currently active. Rendered as a plain DOM overlay OUTSIDE
  // the map's Marker/event layer -- MapLibre's Marker implementation calls
  // preventDefault() on mousedown, which stops the browser from ever focusing an
  // <input>/<select> nested inside one, so a form with real fields can never be a
  // Marker child. (LinkContextPopup is buttons-only and unaffected by this, so it
  // stays a Marker, anchored next to the link it's about.)
  let activeForm: ReactNode = null
  if (pendingMove && selectedLink) {
    const target = draft?.deviceByKey.get(pendingMove.targetKey)
    activeForm = (
      <MoveLinkEndForm
        linkCode={selectedLink.code}
        targetDeviceCode={target?.code ?? pendingMove.targetKey}
        defaultLatencyUs={selectedLink.latency_us}
        defaultBandwidthGbps={selectedLink.bandwidth_bps / 1e9}
        onSubmit={submitMove}
        onCancel={() => setPendingMove(null)}
      />
    )
  } else if (selectedLink && popupMode === 'edit') {
    activeForm = (
      <LinkEditForm
        link={selectedLink}
        onSubmit={handleEditLink}
        onCancel={() => setPopupMode('menu')}
      />
    )
  } else if (addLinkSource && addLinkTarget) {
    const a = draft?.deviceByKey.get(addLinkSource)
    const z = draft?.deviceByKey.get(addLinkTarget)
    activeForm = (
      <AddLinkForm
        sourceCode={a?.code ?? '?'}
        targetCode={z?.code ?? '?'}
        suggestedLatencyUs={Math.round(addLinkSuggestion.latencyNs / 1000)}
        estimateSource={addLinkSuggestion.source}
        onSubmit={submitAddLink}
        onCancel={resetAddLink}
      />
    )
  } else if (placeDeviceAt) {
    activeForm = (
      <AddDeviceForm
        metros={draft?.metros ?? []}
        defaultMetroPk={nearestMetroPk}
        newMetroCoords={placeDeviceAt}
        onSubmit={submitAddDevice}
        onCancel={() => setPlaceDeviceAt(null)}
      />
    )
  }

  return (
    <div className="relative w-full h-full">
      <MapGL
        initialViewState={{ longitude: 0, latitude: 30, zoom: 2 }}
        minZoom={2}
        maxZoom={18}
        mapStyle={mapStyle}
        style={{ width: '100%', height: '100%' }}
        attributionControl={false}
        interactiveLayerIds={['planner-link-hit', 'planner-link-lines']}
        onClick={handleMapClick}
        onMouseMove={(e) => {
          if (tool === 'add-link' && addLinkSource && !addLinkTarget) {
            setCursor([e.lngLat.lng, e.lngLat.lat])
          }
        }}
        cursor={tool === 'add-link' ? 'crosshair' : undefined}
      >
        <Source id="planner-links" type="geojson" data={linkGeoJson}>
          <Layer
            id="planner-link-hit"
            type="line"
            paint={{ 'line-color': 'transparent', 'line-width': 12 }}
            layout={{ 'line-cap': 'round', 'line-join': 'round' }}
          />
          <Layer
            id="planner-link-selection"
            type="line"
            filter={['==', ['get', 'isSelected'], 1]}
            paint={{
              'line-color': '#3b82f6',
              'line-width': ['+', ['get', 'weight'], 8],
              'line-opacity': 0.3,
            }}
            layout={{ 'line-cap': 'round', 'line-join': 'round' }}
          />
          <Layer
            id="planner-link-lines"
            type="line"
            filter={['!=', ['get', 'useDash'], true]}
            paint={{
              'line-color': ['get', 'color'],
              'line-width': ['get', 'weight'],
              'line-opacity': ['get', 'opacity'],
            }}
            layout={{ 'line-cap': 'round', 'line-join': 'round' }}
          />
          <Layer
            id="planner-link-lines-dashed"
            type="line"
            filter={['==', ['get', 'useDash'], true]}
            paint={{
              'line-color': ['get', 'color'],
              'line-width': ['get', 'weight'],
              'line-opacity': ['get', 'opacity'],
              'line-dasharray': [3, 3],
            }}
            layout={{ 'line-cap': 'round', 'line-join': 'round' }}
          />
        </Source>

        {draft?.devices.map((device) => {
          const pos = positions.get(device.pk)
          if (!pos) return null
          const style = deviceChangeStyle(device.changeState, isDark)
          return (
            <Marker key={device.pk} longitude={pos[0]} latitude={pos[1]} anchor="center">
              <div
                className="cursor-pointer"
                title={device.code}
                onClick={() => handleDeviceClick(device.pk)}
              >
                <div
                  className="rounded-full"
                  style={{
                    width: 12,
                    height: 12,
                    backgroundColor: style.color,
                    opacity: style.opacity,
                    border: style.ring ? '2px solid #fff' : '1px solid rgba(0,0,0,0.3)',
                    boxShadow: style.ring ? `0 0 0 2px ${style.color}` : undefined,
                    textDecoration: style.struck ? 'line-through' : undefined,
                  }}
                />
              </div>
            </Marker>
          )
        })}

        {selectedLink && selectedMidpoint && popupMode === 'menu' && (
          <Marker longitude={selectedMidpoint[0]} latitude={selectedMidpoint[1]} anchor="bottom">
            <div onClick={(e) => e.stopPropagation()} className="-translate-y-3">
              <LinkContextPopup
                link={selectedLink}
                onDelete={handleDeleteLink}
                onEdit={() => setPopupMode('edit')}
              />
            </div>
          </Marker>
        )}

        {selectedLink &&
          (['a', 'z'] as const).map((side) => {
            const key = side === 'a' ? selectedLink.side_a_pk : selectedLink.side_z_pk
            const pos = positions.get(key)
            if (!pos) return null
            return (
              <Marker
                key={`handle-${side}`}
                longitude={pos[0]}
                latitude={pos[1]}
                anchor="center"
                draggable
                onDrag={(e) => {
                  // Same exclusion as handleEndpointDragEnd: never preview a snap to
                  // either of this link's own endpoints (no-op / self-loop).
                  const candidates = new Map(positions)
                  candidates.delete(selectedLink.side_a_pk)
                  candidates.delete(selectedLink.side_z_pk)
                  setDragSnapKey(
                    nearestKeyWithin(e.lngLat.lng, e.lngLat.lat, candidates, SNAP_RADIUS_DEG)
                  )
                }}
                onDragEnd={(e) => handleEndpointDragEnd(side, e.lngLat.lng, e.lngLat.lat)}
              >
                <div
                  className="rounded-full border-2 border-white cursor-grab"
                  style={{ width: 14, height: 14, backgroundColor: '#3b82f6' }}
                />
              </Marker>
            )
          })}

        {/* Snap highlight ring on the hovered target device */}
        {dragSnapKey &&
          (() => {
            const pos = positions.get(dragSnapKey)
            if (!pos) return null
            return (
              <Marker longitude={pos[0]} latitude={pos[1]} anchor="center">
                <div
                  className="rounded-full border-2 border-emerald-400 animate-pulse"
                  style={{ width: 22, height: 22 }}
                />
              </Marker>
            )
          })()}

        {tool === 'add-link' && addLinkSource && (
          <Source id="planner-rubber-band" type="geojson" data={rubberBand}>
            <Layer
              id="planner-rubber-band-line"
              type="line"
              paint={{ 'line-color': '#10b981', 'line-width': 2, 'line-dasharray': [2, 2] }}
            />
          </Source>
        )}
      </MapGL>

      {/* Floating overlay for whichever form is active. A normal DOM sibling of the
          map (not a Marker child), positioned top-center so it stays clear of the
          point of interest while still being reachable/focusable like any other
          page content. */}
      {activeForm && (
        <div className="absolute top-4 left-1/2 -translate-x-1/2 z-20">{activeForm}</div>
      )}

      {/* Legend */}
      <div className="absolute bottom-3 left-3 bg-card/90 border border-border rounded-md px-3 py-2 text-xs space-y-1">
        {CHANGE_LEGEND.map((l) => (
          <div key={l.state} className="flex items-center gap-2">
            <span className="inline-block w-3 h-1.5 rounded" style={{ backgroundColor: l.color }} />
            {l.label}
          </div>
        ))}
      </div>
    </div>
  )
}
