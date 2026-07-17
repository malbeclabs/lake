import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { MoveLinkEndForm } from './MoveLinkEndForm'
import { UNSET_LATENCY_NS } from './estimator'
import { PlannerMap } from './PlannerMap'
import { buildDraft, type DraftTopology } from './draft'
import type { TopologyDevice, TopologyLink, TopologyMetro, TopologyResponse } from '@/lib/api'

interface MockPlanner {
  draft: DraftTopology | null
  tool: string
  selectedLinkKey: string | null
  selectLink: (k: string | null) => void
  addChange: (input: unknown) => void
}

// Shared, hoisted refs so the mocked modules below can read values the tests set.
const { plannerRef, dragTargetRef } = vi.hoisted(() => ({
  plannerRef: { current: null as unknown as MockPlanner },
  // Where a simulated endpoint drag "drops" (lng/lat). Tests point this at a device.
  dragTargetRef: { current: { lng: 0, lat: 0 } },
}))

vi.mock('@/hooks/use-theme', () => ({
  useTheme: () => ({ resolvedTheme: 'light', theme: 'light', setTheme: () => {} }),
}))

vi.mock('./PlannerContext', () => ({
  usePlanner: () => plannerRef.current,
}))

// Replace MapLibre with light stand-ins: MapGL/Source pass children through, Layer
// renders nothing, and a draggable Marker becomes a button whose click fires
// onDragEnd at dragTargetRef (so a test can "drag an endpoint onto a device").
vi.mock('react-map-gl/maplibre', () => ({
  __esModule: true,
  default: ({ children }: { children?: React.ReactNode }) => <div>{children}</div>,
  Source: ({ children }: { children?: React.ReactNode }) => <div>{children}</div>,
  Layer: () => null,
  Marker: ({
    children,
    draggable,
    onDragEnd,
  }: {
    children?: React.ReactNode
    draggable?: boolean
    onDragEnd?: (e: { lngLat: { lng: number; lat: number } }) => void
  }) =>
    draggable ? (
      <button
        type="button"
        data-testid="drag-handle"
        onClick={() => onDragEnd?.({ lngLat: dragTargetRef.current })}
      >
        {children}
      </button>
    ) : (
      <div>{children}</div>
    ),
}))

// Three single-device metros, so buildDevicePositions places each device exactly at
// its metro coordinate: dA=[-74,40], dB=[0,51], dC=[2,48].
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

function makeDraft(): DraftTopology {
  const baseline: TopologyResponse = {
    metros: METROS,
    devices: [
      makeDevice('dA', 'nyc-a', 'M1'),
      makeDevice('dB', 'lon-b', 'M2'),
      makeDevice('dC', 'par-c', 'M3'),
    ],
    // LA joins dA-dB; LB joins dA-dC (a different link to switch selection to).
    links: [
      makeTopoLink('LA', 'la-link', 'dA', 'dB'),
      makeTopoLink('LB', 'lb-link', 'dA', 'dC'),
    ],
    validators: [],
  }
  return buildDraft(baseline, [])
}

function makePlanner(selectedLinkKey: string | null, addChange: (i: unknown) => void): MockPlanner {
  return {
    draft: makeDraft(),
    tool: 'select',
    selectedLinkKey,
    selectLink: () => {},
    addChange,
  }
}

describe('MoveLinkEndForm', () => {
  it('shows the link and target device codes, pre-filled latency (ms) and bandwidth (Gbps)', () => {
    render(
      <MoveLinkEndForm
        linkCode="nyc-lon1"
        targetDeviceCode="lon-x2"
        defaultLatencyMs={5}
        defaultBandwidthGbps={10}
        onSubmit={vi.fn()}
        onCancel={vi.fn()}
      />
    )
    expect(screen.getByText(/nyc-lon1/)).toBeInTheDocument()
    expect(screen.getByText(/lon-x2/)).toBeInTheDocument()
    expect(screen.getByLabelText(/Latency/)).toHaveValue(5)
    expect(screen.getByLabelText(/Bandwidth/)).toHaveValue(10)
  })

  it('rounds latency to ns and bandwidth to bps on confirm (no interface field)', () => {
    const onSubmit = vi.fn()
    render(
      <MoveLinkEndForm
        linkCode="nyc-lon1"
        targetDeviceCode="lon-x2"
        defaultLatencyMs={5}
        defaultBandwidthGbps={10}
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    expect(screen.queryByLabelText(/interface/i)).not.toBeInTheDocument()
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '1.5005' } })
    fireEvent.change(screen.getByLabelText(/Bandwidth/), { target: { value: '2.5' } })
    fireEvent.click(screen.getByText('Confirm move'))
    expect(onSubmit).toHaveBeenCalledWith(1_500_500, 2_500_000_000)
  })

  it('does not submit when latency is non-positive', () => {
    const onSubmit = vi.fn()
    render(
      <MoveLinkEndForm
        linkCode="nyc-lon1"
        targetDeviceCode="lon-x2"
        defaultLatencyMs={5}
        defaultBandwidthGbps={10}
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '0' } })
    fireEvent.click(screen.getByText('Confirm move'))
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('does not submit when bandwidth is non-positive', () => {
    const onSubmit = vi.fn()
    render(
      <MoveLinkEndForm
        linkCode="nyc-lon1"
        targetDeviceCode="lon-x2"
        defaultLatencyMs={5}
        defaultBandwidthGbps={10}
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Bandwidth/), { target: { value: '0' } })
    fireEvent.click(screen.getByText('Confirm move'))
    expect(onSubmit).not.toHaveBeenCalled()
  })

  // A latency of exactly 1,000 ms converts to the 1e9-ns sentinel that the
  // impact engine treats as "unset" and silently drops. Never let that value save.
  it('does not submit a latency that equals the unset sentinel (1e9 ns)', () => {
    const onSubmit = vi.fn()
    render(
      <MoveLinkEndForm
        linkCode="nyc-lon1"
        targetDeviceCode="lon-x2"
        defaultLatencyMs={5}
        defaultBandwidthGbps={10}
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    expect(UNSET_LATENCY_NS).toBe(1_000 * 1e6)
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '1000' } })
    fireEvent.click(screen.getByText('Confirm move'))
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('calls onCancel when Cancel is clicked', () => {
    const onCancel = vi.fn()
    render(
      <MoveLinkEndForm
        linkCode="nyc-lon1"
        targetDeviceCode="lon-x2"
        defaultLatencyMs={5}
        defaultBandwidthGbps={10}
        onSubmit={vi.fn()}
        onCancel={onCancel}
      />
    )
    fireEvent.click(screen.getByText('Cancel'))
    expect(onCancel).toHaveBeenCalledTimes(1)
  })
})

// Review round 1 (CRITICAL): a pending move started on one link must never survive a
// selection change and get confirmed against a different link. These render the real
// PlannerMap (with MapLibre stubbed) to exercise the endpoint-drag → snap → move flow.
describe('PlannerMap pending-move reset on link selection change', () => {
  beforeEach(() => {
    dragTargetRef.current = { lng: 0, lat: 0 }
  })

  it('clears the pending move when the selected link changes (no stale confirm against link B)', () => {
    const addChange = vi.fn()
    plannerRef.current = makePlanner('LA', addChange)

    const { rerender } = render(<PlannerMap />)

    // Drag link LA's side-a handle onto device dC (par, at [2,48]); it snaps and opens
    // the move form targeting dC.
    dragTargetRef.current = { lng: 2, lat: 48 }
    fireEvent.click(screen.getAllByTestId('drag-handle')[0])
    expect(screen.getByText(/Move la-link/)).toBeInTheDocument()
    expect(screen.getByText('Confirm move')).toBeInTheDocument()

    // Selection switches to a DIFFERENT link (LB) without confirming.
    plannerRef.current = makePlanner('LB', addChange)
    rerender(<PlannerMap />)

    // The stale move form is gone (pendingMove was cleared), so there is no way to
    // confirm the LA-started move against LB. Nothing was staged.
    expect(screen.queryByText('Confirm move')).not.toBeInTheDocument()
    expect(screen.queryByText(/Move la-link/)).not.toBeInTheDocument()
    expect(addChange).not.toHaveBeenCalled()
  })

  it('stages a move only for the link a fresh drag actually started on', () => {
    const addChange = vi.fn()
    plannerRef.current = makePlanner('LB', addChange)
    render(<PlannerMap />)

    // Fresh drag on LB's side-a handle onto device dB (lon, at [0,51]).
    dragTargetRef.current = { lng: 0, lat: 51 }
    fireEvent.click(screen.getAllByTestId('drag-handle')[0])
    expect(screen.getByText(/Move lb-link/)).toBeInTheDocument()

    fireEvent.click(screen.getByText('Confirm move'))

    expect(addChange).toHaveBeenCalledTimes(1)
    expect(addChange).toHaveBeenCalledWith(
      expect.objectContaining({
        op_type: 'move_link_end',
        ref_link_pk: 'LB',
        new_device_pk: 'dB',
        // The real interface is TBD -- the contributor decides it later.
        payload: expect.objectContaining({ new_iface_name: 'TBD' }),
      })
    )
  })

  it('never snaps an endpoint to its link\'s own other endpoint (no self-loop)', () => {
    const addChange = vi.fn()
    plannerRef.current = makePlanner('LA', addChange)
    render(<PlannerMap />)

    // Drag LA's side-a handle onto LA's OWN side-z device dB ([0,51]). That would make
    // both ends the same device; the endpoint exclusion means it must not snap at all.
    dragTargetRef.current = { lng: 0, lat: 51 }
    fireEvent.click(screen.getAllByTestId('drag-handle')[0])

    expect(screen.queryByText('Confirm move')).not.toBeInTheDocument()
    expect(addChange).not.toHaveBeenCalled()
  })
})
