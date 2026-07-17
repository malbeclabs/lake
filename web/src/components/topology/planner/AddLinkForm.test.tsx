import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { AddLinkForm } from './AddLinkForm'
import { UNSET_LATENCY_NS } from './estimator'
import { PlannerMap } from './PlannerMap'
import { buildDraft, type DraftTopology } from './draft'
import type { TopologyDevice, TopologyLink, TopologyMetro, TopologyResponse } from '@/lib/api'

describe('AddLinkForm', () => {
  it('shows the source/target codes and pre-fills latency (ms) with the estimate source', () => {
    render(
      <AddLinkForm
        sourceCode="nyc-a"
        targetCode="lon-b"
        suggestedLatencyMs={5}
        estimateSource="copied"
        onSubmit={vi.fn()}
        onCancel={vi.fn()}
      />
    )
    expect(screen.getByText('New link nyc-a ↔ lon-b')).toBeInTheDocument()
    expect(screen.getByLabelText(/Latency/)).toHaveValue(5)
    expect(screen.getByText('(copied)')).toBeInTheDocument()
    // Defaults.
    expect(screen.getByLabelText(/Bandwidth/)).toHaveValue(10)
  })

  it('has no interface-name inputs (interface is TBD)', () => {
    render(
      <AddLinkForm
        sourceCode="nyc-a"
        targetCode="lon-b"
        suggestedLatencyMs={5}
        estimateSource="copied"
        onSubmit={vi.fn()}
        onCancel={vi.fn()}
      />
    )
    expect(screen.queryByLabelText(/iface/i)).not.toBeInTheDocument()
  })

  it('requires latency and bandwidth before submitting', () => {
    const onSubmit = vi.fn()
    render(
      <AddLinkForm
        sourceCode="nyc-a"
        targetCode="lon-b"
        suggestedLatencyMs={5}
        estimateSource="copied"
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '0' } })
    fireEvent.click(screen.getByText('Add link'))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText('Latency and bandwidth are required.')).toBeInTheDocument()
  })

  it('rounds latency to ns and bandwidth to bps, and keeps the estimate source when latency is untouched', () => {
    const onSubmit = vi.fn()
    render(
      <AddLinkForm
        sourceCode="nyc-a"
        targetCode="lon-b"
        suggestedLatencyMs={5}
        estimateSource="copied"
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Bandwidth/), { target: { value: '2.5' } })
    fireEvent.click(screen.getByText('Add link'))
    expect(onSubmit).toHaveBeenCalledWith({
      latencyNs: 5_000_000,
      bandwidthBps: 2_500_000_000,
      estimateSource: 'copied',
      linkType: 'WAN',
    })
  })

  it('downgrades the estimate source to manual when the operator changes the pre-filled latency', () => {
    const onSubmit = vi.fn()
    render(
      <AddLinkForm
        sourceCode="nyc-a"
        targetCode="lon-b"
        suggestedLatencyMs={5}
        estimateSource="great_circle"
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '9' } })
    fireEvent.click(screen.getByText('Add link'))
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ latencyNs: 9_000_000, estimateSource: 'manual' })
    )
  })

  it('lets the operator switch the link type to DZX', () => {
    const onSubmit = vi.fn()
    render(
      <AddLinkForm
        sourceCode="nyc-a"
        targetCode="lon-b"
        suggestedLatencyMs={5}
        estimateSource="copied"
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Link type/), { target: { value: 'DZX' } })
    fireEvent.click(screen.getByText('Add link'))
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ linkType: 'DZX' }))
  })

  // Global constraint: a latency of exactly 1,000 ms converts to the 1e9-ns
  // sentinel the impact engine treats as "unset" and silently drops. Same guard as
  // LinkEditForm / MoveLinkEndForm; never let it save from this form either.
  it('rejects a latency that equals the unset sentinel (1e9 ns) and shows an alert', () => {
    const onSubmit = vi.fn()
    render(
      <AddLinkForm
        sourceCode="nyc-a"
        targetCode="lon-b"
        suggestedLatencyMs={5}
        estimateSource="copied"
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    expect(UNSET_LATENCY_NS).toBe(1_000 * 1e6)
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '1000' } })
    fireEvent.click(screen.getByText('Add link'))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })

  it('calls onCancel when Cancel is clicked', () => {
    const onCancel = vi.fn()
    render(
      <AddLinkForm
        sourceCode="nyc-a"
        targetCode="lon-b"
        suggestedLatencyMs={5}
        estimateSource="copied"
        onSubmit={vi.fn()}
        onCancel={onCancel}
      />
    )
    fireEvent.click(screen.getByText('Cancel'))
    expect(onCancel).toHaveBeenCalledTimes(1)
  })
})

// --- PlannerMap rubber-band add-link tool -----------------------------------

interface MockPlanner {
  draft: DraftTopology | null
  baseline: TopologyResponse | null
  tool: string
  setTool: (t: string) => void
  selectedLinkKey: string | null
  selectLink: (k: string | null) => void
  addChange: (input: unknown) => void
}

// Shared, hoisted refs so the mocked modules below can read values the tests set.
const { plannerRef, mapClickTargetRef } = vi.hoisted(() => ({
  plannerRef: { current: null as unknown as MockPlanner },
  // Where a simulated map click "lands" (lng/lat). Add-link picking now geo-snaps
  // off the map's own click (not the device marker's click) -- see PlannerMap's
  // handleMapClick / resolveMapClick -- so tests point this at a device's exact
  // position rather than clicking the device dot directly.
  mapClickTargetRef: { current: { lng: 0, lat: 0 } },
}))

vi.mock('@/hooks/use-theme', () => ({
  useTheme: () => ({ resolvedTheme: 'light', theme: 'light', setTheme: () => {} }),
}))

vi.mock('./PlannerContext', () => ({
  usePlanner: () => plannerRef.current,
}))

// Same light MapLibre stand-ins used by AddDeviceForm.test.tsx: MapGL/Source pass
// children through, Layer renders nothing, non-draggable Marker is a plain div. A
// dedicated "map-surface" sibling (not an ancestor of the device markers) fires the
// MapGL onClick prop, matching how react-map-gl/maplibre keeps marker DOM nodes
// separate from the map canvas's own click handling.
vi.mock('react-map-gl/maplibre', () => ({
  __esModule: true,
  default: ({
    children,
    onClick,
  }: {
    children?: React.ReactNode
    onClick?: (e: { lngLat: { lng: number; lat: number }; features?: unknown[] }) => void
  }) => (
    <div>
      <div
        data-testid="map-surface"
        onClick={() => onClick?.({ lngLat: mapClickTargetRef.current, features: [] })}
      />
      {children}
    </div>
  ),
  Source: ({ children }: { children?: React.ReactNode }) => <div>{children}</div>,
  Layer: () => null,
  Marker: ({
    children,
    draggable,
  }: {
    children?: React.ReactNode
    draggable?: boolean
  }) => (draggable ? <button type="button">{children}</button> : <div>{children}</div>),
}))

// Three single-device metros so buildDevicePositions places each device exactly at
// its metro coordinate: dA=[-74,40] (nyc), dB=[0,51] (lon), dC=[2,48] (par).
const METROS: TopologyMetro[] = [
  { pk: 'M1', code: 'nyc', name: 'New York', latitude: 40, longitude: -74 },
  { pk: 'M2', code: 'lon', name: 'London', latitude: 51, longitude: 0 },
  { pk: 'M3', code: 'par', name: 'Paris', latitude: 48, longitude: 2 },
]

function makeDevice(pk: string, code: string, metroPk: string): TopologyDevice {
  return {
    pk, code, status: 'active', device_type: 'switch', metro_pk: metroPk,
    contributor_pk: 'c1', contributor_code: 'co',
    user_count: 0, unicast_users_count: 0, multicast_subscribers_count: 0,
    multicast_publishers_count: 0, max_unicast_users: 0, max_multicast_subscribers: 0,
    max_multicast_publishers: 0, validator_count: 0, stake_sol: 0, stake_share: 0,
    interfaces: [],
  }
}

function makeTopoLink(pk: string, code: string, aPk: string, zPk: string): TopologyLink {
  return {
    pk, code, status: 'active', link_type: 'WAN', bandwidth_bps: 10_000_000_000,
    side_a_pk: aPk, side_a_code: '', side_a_iface_name: 'eth0', side_a_ip: '',
    side_z_pk: zPk, side_z_code: '', side_z_iface_name: 'eth1', side_z_ip: '',
    contributor_pk: '', contributor_code: '',
    side_a_contributor_pk: '', side_a_contributor_code: '',
    side_z_contributor_pk: '', side_z_contributor_code: '',
    latency_us: 5_000, jitter_us: 0, latency_a_to_z_us: 0, jitter_a_to_z_us: 0,
    latency_z_to_a_us: 0, jitter_z_to_a_us: 0, loss_percent: 0, sample_count: 0,
    in_bps: 0, out_bps: 0, committed_rtt_ns: 5_000_000, isis_delay_override_ns: 0,
  }
}

// Only one link (dA nyc <-> dB lon), so estimateLatencyNs finds a 'copied' comparable
// for the nyc/lon pair but must fall back to great_circle for any pair touching par.
function makeBaseline(): TopologyResponse {
  return {
    metros: METROS,
    devices: [
      makeDevice('dA', 'nyc-a', 'M1'),
      makeDevice('dB', 'lon-b', 'M2'),
      makeDevice('dC', 'par-c', 'M3'),
    ],
    links: [makeTopoLink('LA', 'la-link', 'dA', 'dB')],
    validators: [],
  }
}

function makePlanner(tool: string, setTool: (t: string) => void, addChange: (i: unknown) => void): MockPlanner {
  const baseline = makeBaseline()
  return {
    draft: buildDraft(baseline, []),
    baseline,
    tool,
    setTool,
    selectedLinkKey: null,
    selectLink: () => {},
    addChange,
  }
}

describe('PlannerMap add-link rubber-band tool', () => {
  it('picks source then target by click, prefills latency from the estimator, and stages an add_link', () => {
    const addChange = vi.fn()
    const setTool = vi.fn()
    plannerRef.current = makePlanner('add-link', setTool, addChange)
    render(<PlannerMap />)

    mapClickTargetRef.current = { lng: -74, lat: 40 } // nyc-a
    fireEvent.click(screen.getByTestId('map-surface'))
    mapClickTargetRef.current = { lng: 0, lat: 51 } // lon-b
    fireEvent.click(screen.getByTestId('map-surface'))

    expect(screen.getByText(/New link/)).toBeInTheDocument()
    expect(screen.getByLabelText(/Latency/)).toHaveValue(5)
    expect(screen.getByText('(copied)')).toBeInTheDocument()

    fireEvent.click(screen.getByText('Add link'))

    expect(addChange).toHaveBeenCalledTimes(1)
    expect(addChange).toHaveBeenCalledWith(
      expect.objectContaining({
        op_type: 'add_link',
        local_ref: expect.stringMatching(/^tmp_link_/),
        payload: expect.objectContaining({
          side_a_device_pk: 'dA',
          side_z_device_pk: 'dB',
          // The real interfaces are TBD -- the contributor decides them later.
          side_a_iface_name: 'TBD',
          side_z_iface_name: 'TBD',
          latency_ns: 5_000_000,
          bandwidth_bps: 10_000_000_000,
          estimate_source: 'copied',
          link_type: 'WAN',
        }),
        ref_snapshot: expect.objectContaining({ link_code: 'nyc-a-lon-b' }),
      })
    )
    // Confirming returns to the select tool so the map stops picking devices.
    expect(setTool).toHaveBeenCalledWith('select')
  })

  // CRITICAL (stale-state-across-tool-change): a source device picked in one
  // add-link session must never survive a switch away from the tool and back. If it
  // did, the very next click after re-entering the tool would be silently treated as
  // the TARGET of the stale source, staging a link the operator never intended.
  it('resets a stale source pick when the tool changes away and back', () => {
    const addChange = vi.fn()
    const setTool = vi.fn()
    plannerRef.current = makePlanner('add-link', setTool, addChange)
    const { rerender } = render(<PlannerMap />)

    // Pick nyc-a as the source, then abandon the flow by switching tools (e.g. via
    // the toolbar) without ever picking a target.
    mapClickTargetRef.current = { lng: -74, lat: 40 } // nyc-a
    fireEvent.click(screen.getByTestId('map-surface'))

    plannerRef.current = makePlanner('select', setTool, addChange)
    rerender(<PlannerMap />)
    plannerRef.current = makePlanner('add-link', setTool, addChange)
    rerender(<PlannerMap />)

    // A single click now must start a FRESH source pick (par-c), not resolve as the
    // target of the abandoned nyc-a pick -- so no form yet.
    mapClickTargetRef.current = { lng: 2, lat: 48 } // par-c
    fireEvent.click(screen.getByTestId('map-surface'))
    expect(screen.queryByText(/New link/)).not.toBeInTheDocument()
    expect(addChange).not.toHaveBeenCalled()

    // Completing the pick proves par-c (not the stale nyc-a) is the actual source.
    mapClickTargetRef.current = { lng: 0, lat: 51 } // lon-b
    fireEvent.click(screen.getByTestId('map-surface'))
    expect(screen.getByText('New link par-c ↔ lon-b')).toBeInTheDocument()
    expect(screen.queryByText(/New link nyc-a/)).not.toBeInTheDocument()
  })

  it('cancelling the add-link form clears the pick so the next two clicks start a fresh pair', () => {
    const addChange = vi.fn()
    const setTool = vi.fn()
    plannerRef.current = makePlanner('add-link', setTool, addChange)
    render(<PlannerMap />)

    mapClickTargetRef.current = { lng: -74, lat: 40 } // nyc-a
    fireEvent.click(screen.getByTestId('map-surface'))
    mapClickTargetRef.current = { lng: 0, lat: 51 } // lon-b
    fireEvent.click(screen.getByTestId('map-surface'))
    expect(screen.getByText(/New link/)).toBeInTheDocument()

    fireEvent.click(screen.getByText('Cancel'))
    expect(screen.queryByText(/New link/)).not.toBeInTheDocument()

    mapClickTargetRef.current = { lng: 2, lat: 48 } // par-c
    fireEvent.click(screen.getByTestId('map-surface'))
    expect(screen.queryByText(/New link/)).not.toBeInTheDocument()
    mapClickTargetRef.current = { lng: 0, lat: 51 } // lon-b
    fireEvent.click(screen.getByTestId('map-surface'))
    expect(screen.getByText('New link par-c ↔ lon-b')).toBeInTheDocument()

    expect(addChange).not.toHaveBeenCalled()
  })
})
